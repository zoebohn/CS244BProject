package locks

import(
    "raft"
    "fmt"
    "strings"
    "io"
    "encoding/json"
    "bytes"
    "strconv"
    "sort"
    "sync"
    "time"
)

type MasterFSM struct {
    FsmLock sync.RWMutex
    /* Map of locks to replica group where stored. */
    LockMap             map[Lock]ReplicaGroupId
    /* Map of replica group IDs to server addresses of the servers in that replica group. */
    ClusterMap          map[ReplicaGroupId][]raft.ServerAddress
    /* Map of lock domains to replica group where should be stored. */
    DomainPlacementMap  map[Domain][]ReplicaGroupId
    /* Tracks number of locks held by each replica group. */
    NumLocksHeld        map[ReplicaGroupId]int
    /* Next replica group ID. */
    NextReplicaGroupId  ReplicaGroupId
    /* Location of servers in master cluster. */
    MasterCluster       []raft.ServerAddress
    /* Address of worker clusters to recruit. */
    RecruitAddrs        [][]raft.ServerAddress
    /* Whether to recruit clusters locally - true for testing. */
    RecruitClustersLocally  bool
    /* Threshold at which to rebalance. */
    RebalanceThreshold  int
    /* Eventual destinations of recalcitrant locks. */
    RecalcitrantDestMap map[Lock]ReplicaGroupId
    /* Rebalances in progress */
    RebalancingInProgress map[ReplicaGroupId]bool
    /* Map of lock to average frequency accessed. Maintained with updates from worker. */
    FreqStatsMap          map[Lock]FreqStats
}

type FreqStats struct {
    /* Exponentially weighted moving average of frequency of accesses over period. */
    avgFreq        float64
    /* Time last updated. */
    lastUpdate  time.Time

}

var PERIOD time.Duration = 10 * time.Second

const WEIGHT = 0.2

const numClusterServers = 3

type MasterSnapshot struct{
    json    []byte
}

func CreateMasters (n int, clusterAddrs []raft.ServerAddress, recruitList [][]raft.ServerAddress, rebalanceThreshold int, recruitClustersLocally bool) ([]raft.FSM) {
    masters := make([]*MasterFSM, n)
    for i := range(masters) {
        masters[i] = &MasterFSM {
            LockMap:            make(map[Lock]ReplicaGroupId),
            ClusterMap:         make(map[ReplicaGroupId][]raft.ServerAddress),
            DomainPlacementMap: make(map[Domain][]ReplicaGroupId),
            NumLocksHeld:       make(map[ReplicaGroupId]int),
            NextReplicaGroupId: 0,
            MasterCluster:      clusterAddrs,
            RecruitAddrs:       recruitList,
            RecruitClustersLocally: recruitClustersLocally,
            RebalanceThreshold: rebalanceThreshold,
            RecalcitrantDestMap: make(map[Lock]ReplicaGroupId),
            RebalancingInProgress: make(map[ReplicaGroupId]bool),
            FreqStatsMap:         make(map[Lock]FreqStats),
        }
    }
    if n <= 0 {
        fmt.Println("MASTER: Cannot have number of masters <= 0")
    }
    err := recruitInitialCluster(masters, recruitList[0], recruitClustersLocally)
    if err != nil {
        //TODO
        fmt.Println(err)
    }
    fsms := make([]raft.FSM, n)
    for i, m := range(masters) {
        fsms[i] = m
    }

    return fsms
}

func (m *MasterFSM) Apply(log *raft.Log) (interface{}, func() []byte) {
    /* Interpret log to find command. Call appropriate function. */

    args := make(map[string]string)
    err := json.Unmarshal(log.Data, &args)
    if err != nil {
        fmt.Println("MASTER: error in apply: ", err)
    }
    function := args[FunctionKey]
    switch function {
        case CreateLockCommand:
            l := Lock(args[LockArgKey])
            callback, response := m.createLock(l)
            return response, callback
        case DeleteLockCommand:
            l := Lock(args[LockArgKey])
            callback, response := m.deleteLock(l)
            return response, callback
        case CreateDomainCommand:
            d := Domain(args[DomainArgKey])
            response := m.createLockDomain(d)
            return response, nil
        case LocateLockCommand:
            l := Lock(args[LockArgKey])
            response := m.findLock(l)
            return response, nil
        case ReleasedRecalcitrantCommand:
            l := Lock(args[LockArgKey])
            callback := m.handleReleasedRecalcitrant(l)
            return nil, callback
        case DeleteLockNotAcquiredCommand:
            l := Lock(args[LockArgKey])
            m.deleteLockNotAcquired(l)
            return nil, nil
        case DeleteRecalLockCommand:
            l := Lock(args[LockArgKey])
            m.markLockForDeletion(l)
            return nil, nil
        case FrequencyUpdateCommand:
            lockArr := string_to_lock_array(args[LockArrayKey])
            countArr := string_to_int_array(args[CountArrayKey])
            m.updateFrequencies(lockArr, countArr)
            return nil, nil
        case TransferLockGroupCommand:
            oldGroup, err1 := strconv.Atoi(args[OldGroupKey])
            newGroup, err2 := strconv.Atoi(args[NewGroupKey])
            if err1 != nil || err2 != nil {
                fmt.Println("MASTER: group IDs can't be converted to ints")
                return nil, nil
            }
            lockArr := string_to_lock_array(args[LockArrayKey])
            recalArr := string_to_lock_array(args[LockArray2Key])
            callback := m.initialLockGroupTransfer(ReplicaGroupId(oldGroup), ReplicaGroupId(newGroup), lockArr, recalArr)
            return nil, callback
        case TransferRecalCommand:
            oldGroup, err1 := strconv.Atoi(args[OldGroupKey])
            newGroup, err2 := strconv.Atoi(args[NewGroupKey])
            if err1 != nil || err2 != nil {
                fmt.Println("MASTER: group IDs can't be converted to ints")
                return nil, nil
            }
            l := Lock(args[LockArgKey])
            callback := m.singleRecalcitrantLockTransfer(ReplicaGroupId(oldGroup), ReplicaGroupId(newGroup), l)
            return nil, callback
        }

    return nil, nil
}

func (m *MasterFSM) Snapshot() (raft.FSMSnapshot, error) {
    json, json_err := m.convertToJSON()
    if json_err != nil {
        return MasterSnapshot{json: nil}, json_err
    }
    return MasterSnapshot{json: json}, nil
}

func (m *MasterFSM) Restore(i io.ReadCloser) error {
    var buffer bytes.Buffer
    _, read_err := buffer.ReadFrom(i)
    if read_err != nil {
        return read_err
    }
    snapshotRestored, err := convertFromJSONMaster(buffer.Bytes())
    if err != nil {
        return err
    }
    m.FsmLock.Lock()
    m.LockMap = snapshotRestored.LockMap
    m.ClusterMap = snapshotRestored.ClusterMap
    m.DomainPlacementMap = snapshotRestored.DomainPlacementMap
    m.NumLocksHeld = snapshotRestored.NumLocksHeld
    m.NextReplicaGroupId = snapshotRestored.NextReplicaGroupId
    m.MasterCluster = snapshotRestored.MasterCluster
    m.RecruitAddrs = snapshotRestored.RecruitAddrs
    m.RebalanceThreshold = snapshotRestored.RebalanceThreshold
    m.RecalcitrantDestMap = snapshotRestored.RecalcitrantDestMap
    m.FreqStatsMap = snapshotRestored.FreqStatsMap
    m.FsmLock.Unlock()
    return nil
}

func (s MasterSnapshot) Persist(sink raft.SnapshotSink) error {
    _, err := sink.Write(s.json)
    if err != nil {
        sink.Cancel()
        return err
    }

    sink.Close()
    return nil
}

func (s MasterSnapshot) Release() {
}

func (m *MasterFSM) convertToJSON() ([]byte, error) {
    m.FsmLock.Lock()
    b, err := json.Marshal(m)
    m.FsmLock.Unlock()
    return b, err
}

func convertFromJSONMaster(byte_arr []byte) (MasterFSM, error) {
    var m MasterFSM
    err := json.Unmarshal(byte_arr, &m)
    return m, err
}

func (m *MasterFSM) createLock(l Lock) (func() []byte, CreateLockResponse) {
    m.FsmLock.Lock()
    defer m.FsmLock.Unlock()
    fmt.Println("MASTER: master creating lock with name ", string(l))
    if len(string(l)) == 0 {
        return nil, CreateLockResponse{ErrEmptyPath}
    }
    domain := getParentDomain(string(l))
    replicaGroups, ok := m.DomainPlacementMap[domain]
    if !ok || len(replicaGroups) == 0 {
        return nil, CreateLockResponse{ErrNoIntermediateDomain}
    }
    if _, ok := m.LockMap[l]; ok {
        return nil, CreateLockResponse{ErrLockExists}
    }
    replicaGroup, err := m.choosePlacement(replicaGroups)
    if err != "" {
        return nil, CreateLockResponse{err}
    }
    fmt.Println("MASTER: put lock ", string(l), " in domain", string(domain))
    m.NumLocksHeld[replicaGroup]++
    m.LockMap[l] = replicaGroup
    m.FreqStatsMap[l] = FreqStats{lastUpdate: time.Now(), avgFreq: 0}

    /* Trigger rebalancing if number of locks held by replica group >= rebalance threshold. */
    var rebalanceCallback func() []byte
    if m.NumLocksHeld[replicaGroup] >= m.RebalanceThreshold {
        if _, ok := m.RebalancingInProgress[replicaGroup]; !ok {
            rebalanceCallback = m.rebalance(replicaGroup)
        }
    }

    f := func() []byte {
            m.askWorkerToClaimLocks(replicaGroup, []Lock{l})
            if rebalanceCallback != nil {
                return rebalanceCallback()
            }
            return nil
    }

    return f, CreateLockResponse{Success}
}

func (m *MasterFSM) deleteLock(l Lock) (func() []byte, DeleteLockResponse) {
    m.FsmLock.Lock()
    defer m.FsmLock.Unlock()
    fmt.Println("MASTER: master deleting lock ", string(l))
    replicaGroup, ok := m.LockMap[l]
    if !ok {
        return nil, DeleteLockResponse{ErrLockDoesntExist}
    }
    triggerDelete := func()[]byte {
        fmt.Println("MASTER: delete lock " + l)
        delete_func := func() []byte {
            recalcitrantLocks := m.initiateRebalance(replicaGroup, []Lock{l})
            args := make(map[string]string)
            if len(recalcitrantLocks) == 0 {
                /* Lock is not acquired, can be safely deleted. */
                args[FunctionKey] = DeleteLockNotAcquiredCommand
            } else {
                /* Lock is acquired, mark as recalcitrant and wait for release to delete. */
                args[FunctionKey] = DeleteRecalLockCommand
            }
            args[LockArgKey] = string(l)
            command, json_err := json.Marshal(args)
            if json_err != nil {
                fmt.Println("MASTER: json error")
            }
            return command
        }
        return delete_func()
    }
    return triggerDelete, DeleteLockResponse{Success}
}

func (m *MasterFSM) createLockDomain(d Domain) CreateDomainResponse {
    m.FsmLock.Lock()
    defer m.FsmLock.Unlock()
    fmt.Println("MASTER: master creating domain ", string(d))
    if len(string(d)) == 0 {
        return CreateDomainResponse{ErrEmptyPath}
    }
    if strings.Compare(string(d), "/") == 0 {
        return CreateDomainResponse{ErrDomainExists}
    }
    domain := getParentDomain(string(d))
    replicaGroups, ok := m.DomainPlacementMap[domain]
    if !ok || len(replicaGroups) == 0 {
        return CreateDomainResponse{ErrNoIntermediateDomain}
    }
    if string(d[0]) != "/" {
        d = "/" + d
    }
    if _, ok := m.DomainPlacementMap[d]; ok {
        return CreateDomainResponse{ErrDomainExists}
    }
    replicaGroup, err := m.choosePlacement(replicaGroups)
    if err != "" {
        return CreateDomainResponse{err}
    }
    m.DomainPlacementMap[d] = []ReplicaGroupId{replicaGroup}
    return CreateDomainResponse{""}
}

func (m *MasterFSM) findLock(l Lock) (LocateLockResponse) {
    m.FsmLock.RLock()
    defer m.FsmLock.RUnlock()
    replicaGroup, ok := m.LockMap[l]
    if !ok {
        return LocateLockResponse{-1, nil, ErrLockDoesntExist}
    }
    response := LocateLockResponse{replicaGroup, m.ClusterMap[replicaGroup], ""}
    return response 
}

func getParentDomain(path string) Domain {
    split := strings.Split(path, "/")
    /* Set root as parent of all directories */
    slice := []string{}
    for _, s := range(split) {
        if s == "" {
            continue
        }
        slice = append(slice, s)
    }
    /* Remove last element. */
    slice = slice[:len(slice)-1]
    return Domain("/" + strings.Join(slice, "/"))
}

func (m *MasterFSM) choosePlacement(replicaGroups []ReplicaGroupId) (ReplicaGroupId, string) {
    if len(replicaGroups) == 0 {
        return -1, ErrNoPlacement
    }
    chosen := replicaGroups[0]
    minLoad := m.NumLocksHeld[chosen]
    for _, replicaGroup := range(replicaGroups) {
        if minLoad > m.NumLocksHeld[replicaGroup] {
            chosen = replicaGroup
            minLoad = m.NumLocksHeld[replicaGroup]
        }
    }
    if chosen == -1 {
        return -1, ErrNoPlacement
    }
    return chosen, "" 
}

func recruitInitialCluster(masters []*MasterFSM, workerAddrs []raft.ServerAddress, recruit_locally bool) (error) {
    if recruit_locally {
        MakeCluster(numClusterServers, CreateWorkers(len(workerAddrs), masters[0].MasterCluster), workerAddrs)
    }
    id := masters[0].NextReplicaGroupId
    for i := range(masters) {
        masters[i].ClusterMap[masters[i].NextReplicaGroupId] = workerAddrs
        masters[i].NumLocksHeld[masters[i].NextReplicaGroupId] = 0
        masters[i].NextReplicaGroupId++
    }
    for i := range(masters) {
        masters[i].DomainPlacementMap["/"] = []ReplicaGroupId{id}
    }
    return nil
}

func (m *MasterFSM) deleteLockNotAcquired(l Lock) func() []byte {
    m.FsmLock.Lock()
    defer m.FsmLock.Unlock()
    replicaGroup := m.LockMap[l]
    delete(m.LockMap, l)
    delete(m.FreqStatsMap, l)
    m.NumLocksHeld[replicaGroup]--
    f := func() []byte {
        m.askWorkerToDisownLocks(replicaGroup, []Lock{l})
        return nil
    }
    return f
    // TODO: check if can fully "retire" cluster
}

func (m* MasterFSM) markLockForDeletion(l Lock) {
    m.FsmLock.Lock()
    defer m.FsmLock.Unlock()
    m.RecalcitrantDestMap[l] = -1
    // TODO: call for rebalance here, make rebalance more generic to join or split
    // if not using a cluster any more, need to make sure don't continue sending locks there (cluster is "retiring")
}

func (m *MasterFSM) rebalance(replicaGroup ReplicaGroupId) func() []byte {
    fmt.Println("MASTER: REBALANCING")
    /* Split locks managed by worker into 2. */
    locksToMove := m.getLocksToRebalance(replicaGroup)
    /* Update state in preparation for adding new cluster. */
    workerAddrs := m.RecruitAddrs[m.NextReplicaGroupId]
    newReplicaGroup := m.NextReplicaGroupId
    m.ClusterMap[newReplicaGroup] = workerAddrs
    m.NumLocksHeld[newReplicaGroup] = 0
    m.NextReplicaGroupId++
    m.RebalancingInProgress[replicaGroup] = true
    rebalancing_func := func() []byte {
        /* Initiate rebalancing and find recalcitrant locks. */
        recalcitrantLocks := m.initiateRebalance(replicaGroup, locksToMove)

        recalcitrantLocksList := make([]Lock, 0)
        for l := range(recalcitrantLocks) {
            recalcitrantLocksList = append(recalcitrantLocksList, l)
        }

        /* Recruit new replica group to store rebalanced locks. */
        if (m.RecruitClustersLocally) {
            MakeCluster(numClusterServers, CreateWorkers(len(workerAddrs), m.MasterCluster), workerAddrs)
        }

        /* Using set of recalcitrant locks, determine locks that can be moved. */
        locksCanMove := make([]Lock, 0)
        for _, currLock := range(locksToMove) {
            if _, ok := recalcitrantLocks[currLock]; !ok {
                locksCanMove = append(locksCanMove, currLock)
            }
        }

        /* Ask new replica group to claim set of locks that can be moved. */
        m.askWorkerToClaimLocks(newReplicaGroup, locksCanMove)

        /* Tell master to transfer ownership of locks from old group to new group. */
        args := make(map[string]string)
        args[FunctionKey] = TransferLockGroupCommand
        args[LockArrayKey] = lock_array_to_string(locksCanMove)
        args[LockArray2Key] = lock_array_to_string(recalcitrantLocksList)
        args[OldGroupKey] = strconv.Itoa(int(replicaGroup))
        args[NewGroupKey] = strconv.Itoa(int(newReplicaGroup))
        command, json_err := json.Marshal(args)
        if json_err != nil {
            fmt.Println("MASTER: json error")
        }
        return command
        }

    return rebalancing_func
}

func (m *MasterFSM) genericClusterRequest(replicaGroup ReplicaGroupId, args map[string]string, resp *raft.ClientResponse) {
    command, json_err := json.Marshal(args)
    if json_err != nil {
        fmt.Println("MASTER: JSON ERROR")
    }
    m.FsmLock.RLock()
    send_err := raft.SendSingletonRequestToCluster(m.ClusterMap[replicaGroup], command, resp)
    m.FsmLock.RUnlock()
    if send_err != nil {
       fmt.Println("MASTER: send err ", send_err)
    }
}

func (m *MasterFSM) initiateRebalance(replicaGroup ReplicaGroupId, locksToMove []Lock) map[Lock]int {
    /* Send RPC to worker with locks_to_move */
    args := make(map[string]string)
    args[FunctionKey] = RebalanceCommand
    args[LockArrayKey] = lock_array_to_string(locksToMove)
    resp := raft.ClientResponse{}
    m.genericClusterRequest(replicaGroup, args, &resp)
    var response RebalanceResponse
    unmarshal_err := json.Unmarshal(resp.ResponseData, &response)
    if unmarshal_err != nil {
        fmt.Println("MASTER: error unmarshalling")
    }
    return response.RecalcitrantLocks
}

func (m *MasterFSM) askWorkerToClaimLocks(replicaGroup ReplicaGroupId, movingLocks []Lock) {
    /* Send RPC to worker with locks to claim. */
    fmt.Println("MASTER: ask worker to claim locks\n")
    args := make(map[string]string)
    args[FunctionKey] = ClaimLocksCommand
    args[LockArrayKey] = lock_array_to_string(movingLocks)
    m.genericClusterRequest(replicaGroup, args, &raft.ClientResponse{})
    // TODO: do we need the claimed locks anywhere?
}

func (m *MasterFSM) askWorkerToDisownLocks(replicaGroup ReplicaGroupId, movingLocks []Lock) {
    /* Send RPC to worker with locks to claim. */
    fmt.Println("MASTER: ask worker to disown locks")
    args := make(map[string]string)
    args[FunctionKey] = DisownLocksCommand
    args[LockArrayKey] = lock_array_to_string(movingLocks)
    m.genericClusterRequest(replicaGroup, args, &raft.ClientResponse{}) 
}

/* Transfer ownership of locks in master and tell old replica group to disown locks. Should only be called after new replica group owns locks. */
func (m *MasterFSM) initialLockGroupTransfer(oldGroupId ReplicaGroupId, newGroupId ReplicaGroupId, movingLocks []Lock, recalcitrantLocks []Lock) func() []byte {
    m.FsmLock.Lock()
    defer m.FsmLock.Unlock()
    /* Update master state to show that locks have moved. */
    m.NumLocksHeld[oldGroupId] -= len(movingLocks)
    for _, l := range(movingLocks) {
        m.LockMap[l] = newGroupId
    }
    m.NumLocksHeld[newGroupId] += len(movingLocks)

    /* Mark eventual destination of recalcitrant locks. */
    for _, l := range(recalcitrantLocks) {
        m.RecalcitrantDestMap[l] = newGroupId
    }

    /* Update domain placement map. */
    /* Use moving locks and recalcitrant locks to find all locks that should eventually move. */
    locksShouldMove := make(map[Lock]int)
    for _, l := range(movingLocks) {
        locksShouldMove[l] = 1
    }
    for _, l := range(recalcitrantLocks) {
        locksShouldMove[l] = 1
    }

    /* Find domains where we should continue to place at oldGroupId. */
    remainingDomains := make(map[Domain]int)
    for l := range(m.LockMap) {
        if m.LockMap[l] == oldGroupId {
            if _, ok := locksShouldMove[l]; !ok {
                remainingDomains[getParentDomain(string(l))] = 1
            }
        }
    }
    /* Find moving domains where we should now place at newGroupId. */
    movingDomains := make(map[Domain]int)
    for l := range(locksShouldMove) {
        movingDomains[getParentDomain(string(l))] = 1
    }
    /* For moving domains, add newGroupId to domainPlacementMap. If an old group no longer holds a domain, remove it from that entry in the domainPlacementMap. */
    for d := range(movingDomains) {
        m.DomainPlacementMap[d] = append(m.DomainPlacementMap[d], newGroupId)
        if _, ok := remainingDomains[d]; !ok {
            i := 0
            for i < len(m.DomainPlacementMap[d]) {
                if m.DomainPlacementMap[d][i] == oldGroupId {
                    if i+1 < len(m.DomainPlacementMap[d]) {
                        m.DomainPlacementMap[d] = append(m.DomainPlacementMap[d][:i], m.DomainPlacementMap[d][i+1:]...)
                    } else {
                        m.DomainPlacementMap[d] = m.DomainPlacementMap[d][:i]
                    }
                } else {
                    i++
                }
            }
        }
    }

    /* No longer rebalancing */
    if _, ok := m.RebalancingInProgress[oldGroupId]; ok {
        delete(m.RebalancingInProgress, oldGroupId)
    }

    /* Ask old replica group to disown locks being moved. */
    f := func() []byte {
        m.askWorkerToDisownLocks(oldGroupId, movingLocks)
        return nil
    }
    return f
}

func (m *MasterFSM) singleRecalcitrantLockTransfer(oldGroupId ReplicaGroupId, newGroupId ReplicaGroupId, l Lock) func() []byte {
    m.FsmLock.Lock()
    m.FsmLock.Unlock()
    /* Update state for transferring recalcitrant lock. */
    m.LockMap[l] = newGroupId
    m.NumLocksHeld[newGroupId]++
    m.NumLocksHeld[oldGroupId]--

    /* Ask worker to disown lock now that transferred. */
    f := func() []byte {
        m.askWorkerToDisownLocks(oldGroupId, []Lock{l})
        return nil
    }

    return f
}

func (m *MasterFSM) getLocksToRebalance(replicaGroup ReplicaGroupId) ([]Lock) {
    /* Find all domains for locks in replica group. */
    locks := make([]string, 0)
    for l := range(m.LockMap) {
        if m.LockMap[l] == replicaGroup {
            locks = append(locks, string(l))
        }
    }
    sort.Strings(locks)

    splitLocks := make([]Lock, 0)
    for i := range(locks) {
        if i >= m.RebalanceThreshold / 2 {
            return splitLocks
        }
        splitLocks = append(splitLocks, Lock(locks[i]))
    }
    fmt.Println("MASTER: error in splitting locks")
    return splitLocks
}

func (m *MasterFSM) handleReleasedRecalcitrant(l Lock) func() []byte {
   /* Find replica group to place lock into, remove recalcitrant lock entry in map. */
   m.FsmLock.Lock()
   defer m.FsmLock.Unlock()
   //m.FsmLock.RLock()
   newReplicaGroup := m.RecalcitrantDestMap[l]
   delete(m.RecalcitrantDestMap, l)
   /* Check if should delete lock. */
   if newReplicaGroup == -1 {
        replicaGroup := m.LockMap[l]
        m.NumLocksHeld[replicaGroup]--
        delete(m.LockMap, l)
        delete(m.FreqStatsMap, l)
        f := func() []byte {
            m.askWorkerToDisownLocks(replicaGroup, []Lock{l})
            return nil
        }
        return f 
   }
   //m.FsmLock.RUnlock()

   sendLockFunc := func() []byte {
       m.FsmLock.RLock()
       fmt.Println("MASTER: send ", l, " to ", newReplicaGroup, " at ", m.ClusterMap[newReplicaGroup])
       m.FsmLock.RUnlock()
       m.askWorkerToClaimLocks(newReplicaGroup, []Lock{l})

       /* Tell master to transfer ownership of locks. */
       args := make(map[string]string)
       args[FunctionKey] = TransferRecalCommand
       args[LockArgKey] = string(l)
       args[OldGroupKey] = strconv.Itoa(int(m.LockMap[l]))
       args[NewGroupKey] = strconv.Itoa(int(newReplicaGroup))
       command, json_err := json.Marshal(args)
       if json_err != nil {
           fmt.Println("MASTER: json error")
       }
       return command
   }

   return sendLockFunc
}

func (m *MasterFSM) updateFrequencies(lockArr []Lock, countArr []int) {
    m.FsmLock.Lock()
    defer m.FsmLock.Unlock()
    currTime := time.Now()
    fmt.Println("Update freqs")
    for i := range lockArr {
        /* Calculate new exponentially weighted moving average. */
        stats := m.FreqStatsMap[lockArr[i]]
        numPeriodsElapsed := float64(time.Since(stats.lastUpdate) / PERIOD)
        newFreq := float64(countArr[i]) / numPeriodsElapsed
        currAvgFreq := stats.avgFreq
        for i := 0; i < int(numPeriodsElapsed); i++ {
            currAvgFreq = (newFreq * WEIGHT * numPeriodsElapsed) + (currAvgFreq * (1 - WEIGHT))
        }
        fmt.Println("frequency of ", lockArr[i], " is ", currAvgFreq)
        newStats := FreqStats{avgFreq: currAvgFreq, lastUpdate: currTime}
        m.FreqStatsMap[lockArr[i]] = newStats
    }
    fmt.Println("Returned from update freqs")
}
