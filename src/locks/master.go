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
)

type MasterFSM struct {
    fsmLock sync.RWMutex
    /* Map of locks to replica group where stored. */
    lockMap             map[Lock]ReplicaGroupId
    /* Map of replica group IDs to server addresses of the servers in that replica group. */
    clusterMap          map[ReplicaGroupId][]raft.ServerAddress
    /* Map of lock domains to replica group where should be stored. */
    domainPlacementMap  map[Domain][]ReplicaGroupId
    /* Tracks number of locks held by each replica group. */
    numLocksHeld        map[ReplicaGroupId]int
    /* Next replica group ID. */
    nextReplicaGroupId  ReplicaGroupId
    /* Location of servers in master cluster. */
    masterCluster       []raft.ServerAddress
    /* Address of worker clusters to recruit. */
    recruitAddrs        [][]raft.ServerAddress
    /* Threshold at which to rebalance. */
    rebalanceThreshold  int
    /* Eventual destinations of recalcitrant locks. */
    recalcitrantDestMap map[Lock]ReplicaGroupId
    /* Rebalances in progress */
    rebalancingInProgress map[ReplicaGroupId]bool
}


/* Constants for recruiting new clusters. */
//var recruitAddrs [][]raft.ServerAddress = [][]raft.ServerAddress{{"127.0.0.1:6000", "127.0.0.1:6001", "127.0.0.1:6002"}, {"127.0.0.1:6003", "127.0.0.1:6004", "127.0.0.1:6005"}, {"127.0.0.1:6006", "127.0.0.1:6007", "127.0.0.1:6008", "127.0.0.1:6009"}, {"127.0.0.1:6010", "127.0.0.1:6011", "127.0.0.1:6012"}, {"127.0.0.1:6013", "127.0.0.1:6014", "127.0.0.1:6015"}, {"127.0.0.1:6016", "127.0.0.1:6017", "127.0.0.1:6018"}, {"127.0.0.1:6019", "127.0.0.1:6020", "127.0.0.1:6021"}}
const numClusterServers = 3

/* Constants for rebalancing */
//const REBALANCE_THRESHOLD = 7 // was 4 
const RECRUIT_CLUSTER_LOCALLY = true 

/* TODO: what do we need to do here? */
type MasterSnapshot struct{
    LockMap             map[Lock]ReplicaGroupId
    ClusterMap          map[ReplicaGroupId][]raft.ServerAddress
    DomainPlacementMap  map[Domain][]ReplicaGroupId
    NumLocksHeld        map[ReplicaGroupId]int
    NextReplicaGroupId  ReplicaGroupId
    MasterCluster       []raft.ServerAddress
    RecruitAddrs        [][]raft.ServerAddress
    RebalanceThreshold  int
    RecalcitrantDestMap map[Lock]ReplicaGroupId
    RebalancingInProgress map[ReplicaGroupId]bool
}

func CreateMasters (n int, clusterAddrs []raft.ServerAddress, recruitList [][]raft.ServerAddress, rebalanceThreshold int) ([]raft.FSM) {
    masters := make([]*MasterFSM, n)
    for i := range(masters) {
        masters[i] = &MasterFSM {
            lockMap:            make(map[Lock]ReplicaGroupId),
            clusterMap:         make(map[ReplicaGroupId][]raft.ServerAddress),
            domainPlacementMap: make(map[Domain][]ReplicaGroupId),
            numLocksHeld:       make(map[ReplicaGroupId]int),
            nextReplicaGroupId: 0,
            masterCluster:      clusterAddrs,
            recruitAddrs:       recruitList,
            rebalanceThreshold: rebalanceThreshold,
            recalcitrantDestMap: make(map[Lock]ReplicaGroupId),
            rebalancingInProgress: make(map[ReplicaGroupId]bool),
        }
    }
    if n <= 0 {
        fmt.Println("MASTER: Cannot have number of masters <= 0")
    }
    err := recruitInitialCluster(masters, recruitList[0])
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

/* TODO: Do we need some kind of FSM init? like a flag that's set for if it's inited and then otherwise we init on first request? */
func (m *MasterFSM) Apply(log *raft.Log) (interface{}, func() []byte) {
    /* Interpret log to find command. Call appropriate function. */

    args := make(map[string]string)
    err := json.Unmarshal(log.Data, &args)
    if err != nil {
        //TODO
        fmt.Println("MASTER: error in apply")
        fmt.Println(err)
    }
    function := args[FunctionKey]
    switch function {
        case CreateLockCommand:
            l := Lock(args[LockArgKey])
            callback, response := m.createLock(l)
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

/* TODO: what to do here? */
func (m *MasterFSM) Snapshot() (raft.FSMSnapshot, error) {
    m.fsmLock.RLock() 
    s := MasterSnapshot{LockMap: m.lockMap, ClusterMap: m.clusterMap,
         DomainPlacementMap: m.domainPlacementMap, NumLocksHeld: m.numLocksHeld,
         NextReplicaGroupId: m.nextReplicaGroupId, MasterCluster: m.masterCluster, RecruitAddrs: m.recruitAddrs, RebalanceThreshold: m.rebalanceThreshold, RecalcitrantDestMap: m.recalcitrantDestMap}
    m.fsmLock.RUnlock()
    return s, nil
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
    m.fsmLock.Lock()
    m.lockMap = snapshotRestored.LockMap
    m.clusterMap = snapshotRestored.ClusterMap
    m.domainPlacementMap = snapshotRestored.DomainPlacementMap
    m.numLocksHeld = snapshotRestored.NumLocksHeld
    m.nextReplicaGroupId = snapshotRestored.NextReplicaGroupId
    m.masterCluster = snapshotRestored.MasterCluster
    m.recruitAddrs = snapshotRestored.RecruitAddrs
    m.rebalanceThreshold = snapshotRestored.RebalanceThreshold
    m.recalcitrantDestMap = snapshotRestored.RecalcitrantDestMap
    m.fsmLock.Unlock()
    return nil
}

func (s MasterSnapshot) Persist(sink raft.SnapshotSink) error {
    /* TODO needs to be safe to invoke this with concurrent apply - they actually lock it in there implementation */
    json, json_err := s.convertToJSON()
    if json_err != nil {
        return json_err
    }
    /* Open sink first? */
    _, err := sink.Write(json)
    if err != nil {
        sink.Cancel()
        return err
    }

    sink.Close()
    return nil
}

func (s MasterSnapshot) Release() {
}

func (s MasterSnapshot) convertToJSON() ([]byte, error) {
    b, err := json.Marshal(s)
    return b, err
}

func convertFromJSONMaster(byte_arr []byte) (MasterSnapshot, error) {
    var s MasterSnapshot
    err := json.Unmarshal(byte_arr, &s)
    return s, err
}

func (m *MasterFSM) createLock(l Lock) (func() []byte, CreateLockResponse) {
    m.fsmLock.Lock()
    fmt.Println("MASTER: master creating lock with name ", string(l))
    fmt.Println("MASTER: Lock map ", m.lockMap)
    /* Check if already eddxists (return false).
      Check that intermediate domains exist. 
      Get replica group ID where should be put.
      Tell replica group to make that log
      numLocksHeld[group]++
      Add lock to lockMap */
    // TODO: sanitize lock name?
    if _, ok := m.lockMap[l]; ok {
        m.fsmLock.Unlock()
        return nil, CreateLockResponse{ErrLockExists}
    }
    if len(string(l)) == 0 {
        m.fsmLock.Unlock()
        return nil, CreateLockResponse{ErrEmptyPath}
    }
    domain := getParentDomain(string(l))
    fmt.Println("MASTER: put lock ", string(l), " in domain", string(domain))
    replicaGroups, ok := m.domainPlacementMap[domain]
    if !ok || len(replicaGroups) == 0 {
        m.fsmLock.Unlock()
        return nil, CreateLockResponse{ErrNoIntermediateDomain}
    }
    replicaGroup, err := m.choosePlacement(replicaGroups)
    if err != "" {
        m.fsmLock.Unlock()
        return nil, CreateLockResponse{err}
    }
    m.numLocksHeld[replicaGroup]++
    m.lockMap[l] = replicaGroup
    
    var rebalanceCallback func() []byte
    if m.numLocksHeld[replicaGroup] >= m.rebalanceThreshold {
        if _, ok := m.rebalancingInProgress[replicaGroup]; !ok {
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

    m.fsmLock.Unlock()
    return f, CreateLockResponse{""}
}

func (m *MasterFSM) createLockDomain(d Domain) CreateDomainResponse {
    m.fsmLock.Lock()
    fmt.Println("MASTER: master creating domain ", string(d))
    /* Check if already exists (return false).
       Check that intermediate domains exist.
       Get replica group ID where should be put.
       Add to domainPlacementMap. */
    if _, ok := m.domainPlacementMap[d]; ok {
        m.fsmLock.Unlock()
        return CreateDomainResponse{ErrDomainExists}
    }
    if len(string(d)) == 0 {
        m.fsmLock.Unlock()
        return CreateDomainResponse{ErrEmptyPath}
    }
    domain := getParentDomain(string(d))
    replicaGroups, ok := m.domainPlacementMap[domain]
    if !ok || len(replicaGroups) == 0 {
        m.fsmLock.Unlock()
        return CreateDomainResponse{ErrNoIntermediateDomain}
    }
    replicaGroup, err := m.choosePlacement(replicaGroups)
    if err != "" {
        m.fsmLock.Unlock()
        return CreateDomainResponse{err}
    }
    m.domainPlacementMap[d] = []ReplicaGroupId{replicaGroup}
    m.fsmLock.Unlock()
    return CreateDomainResponse{""}
}

func (m *MasterFSM) findLock(l Lock) (LocateLockResponse) {
    /* Check that lock exists.
       Check lockMap to find replica group ID. 
       Return replica groupID and servers using clusterMap. */
    m.fsmLock.RLock()
    replicaGroup, ok := m.lockMap[l]
    fmt.Println("LOCK MAP: ", m.lockMap)
    if !ok {
        m.fsmLock.RUnlock()
        return LocateLockResponse{-1, nil, ErrLockDoesntExist}
    }
    response := LocateLockResponse{replicaGroup, m.clusterMap[replicaGroup], ""}
    m.fsmLock.RUnlock()
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
    minLoad := m.numLocksHeld[chosen]
    for _, replicaGroup := range(replicaGroups) {
        if minLoad > m.numLocksHeld[replicaGroup] {
            chosen = replicaGroup
            minLoad = m.numLocksHeld[replicaGroup]
        }
    }
    if chosen == -1 {
        return -1, ErrNoPlacement
    }
    return chosen, "" 
}

func recruitInitialCluster(masters []*MasterFSM, workerAddrs []raft.ServerAddress) (error) {
    if RECRUIT_CLUSTER_LOCALLY {
        MakeCluster(numClusterServers, CreateWorkers(len(workerAddrs), masters[0].masterCluster), workerAddrs)
    }
    id := masters[0].nextReplicaGroupId //TODO race condition?
    for i := range(masters) {
        masters[i].clusterMap[masters[i].nextReplicaGroupId] = workerAddrs
        masters[i].numLocksHeld[masters[i].nextReplicaGroupId] = 0
        masters[i].nextReplicaGroupId++
    }
    for i := range(masters) {
        masters[i].domainPlacementMap["/"] = []ReplicaGroupId{id}
    }
    return nil
}

func (m *MasterFSM) rebalance(replicaGroup ReplicaGroupId) func() []byte {
    m.fsmLock.Lock()
    fmt.Println("MASTER: REBALANCING")
    /* Split managed locks into 2 - tell worker */
    locksToMove := m.getLocksToRebalance(replicaGroup)
    /* Update state in preparation for adding new cluster. */
    fmt.Println("next replica group ID: ", m.nextReplicaGroupId)
    workerAddrs := m.recruitAddrs[m.nextReplicaGroupId]
    newReplicaGroup := m.nextReplicaGroupId
    m.clusterMap[newReplicaGroup] = workerAddrs
    m.numLocksHeld[newReplicaGroup] = 0
    m.nextReplicaGroupId++
    m.rebalancingInProgress[replicaGroup] = true
    m.fsmLock.Unlock()
    rebalancing_func := func() []byte {
        fmt.Println("calling rebalancing callback...")
        /* Initiate rebalancing and find recalcitrant locks. */
        recalcitrantLocks := m.initiateRebalance(replicaGroup, locksToMove)

        recalcitrantLocksList := make([]Lock, 0)
        for l := range(recalcitrantLocks) {
            recalcitrantLocksList = append(recalcitrantLocksList, l)
        }

        /* Recruit new replica group to store rebalanced locks. */
        if (RECRUIT_CLUSTER_LOCALLY) {
            MakeCluster(numClusterServers, CreateWorkers(len(workerAddrs), m.masterCluster), workerAddrs)
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
        //TODO
        fmt.Println("MASTER: JSON ERROR")
    }
    m.fsmLock.RLock()
    send_err := raft.SendSingletonRequestToCluster(m.clusterMap[replicaGroup], command, resp)
    m.fsmLock.RUnlock()
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
    m.fsmLock.Lock()
    /* Update master state to show that locks have moved. */
    m.numLocksHeld[oldGroupId] -= len(movingLocks)
    for _, l := range(movingLocks) {
        m.lockMap[l] = newGroupId
    }
    m.numLocksHeld[newGroupId] += len(movingLocks)

    /* Mark eventual destination of recalcitrant locks. */
    for _, l := range(recalcitrantLocks) {
        m.recalcitrantDestMap[l] = newGroupId
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
    for l := range(m.lockMap) {
        if m.lockMap[l] == oldGroupId {
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
        m.domainPlacementMap[d] = append(m.domainPlacementMap[d], newGroupId)
        if _, ok := remainingDomains[d]; !ok {
            i := 0
            for i < len(m.domainPlacementMap[d]) {
                if m.domainPlacementMap[d][i] == oldGroupId {
                    if i+1 < len(m.domainPlacementMap[d]) {
                        m.domainPlacementMap[d] = append(m.domainPlacementMap[d][:i], m.domainPlacementMap[d][i+1:]...)
                    } else {
                        m.domainPlacementMap[d] = m.domainPlacementMap[d][:i]
                    }
                } else {
                    i++
                }
            }
        }
    }

    /* No longer rebalancing */
    if _, ok := m.rebalancingInProgress[oldGroupId]; ok {
        delete(m.rebalancingInProgress, oldGroupId)
    }

    /* Ask old replica group to disown locks being moved. */
    f := func() []byte {
        m.askWorkerToDisownLocks(oldGroupId, movingLocks)
        m.fsmLock.Unlock()
        return nil
    }
    m.fsmLock.Unlock()
    return f
}

func (m *MasterFSM) singleRecalcitrantLockTransfer(oldGroupId ReplicaGroupId, newGroupId ReplicaGroupId, l Lock) func() []byte {
    m.fsmLock.Lock()
    /* Update state for transferring recalcitrant lock. */
    m.lockMap[l] = newGroupId
    m.numLocksHeld[newGroupId]++
    m.numLocksHeld[oldGroupId]--

    /* Ask worker to disown lock now that transferred. */
    f := func() []byte {
        m.askWorkerToDisownLocks(oldGroupId, []Lock{l})
        m.fsmLock.Unlock()
        return nil
    }

    m.fsmLock.Unlock()
    return f
}

func (m *MasterFSM) getLocksToRebalance(replicaGroup ReplicaGroupId) ([]Lock) {
    /* Find all domains for locks in replica group. */
    locks := make([]string, 0)
    for l := range(m.lockMap) {
        if m.lockMap[l] == replicaGroup {
            locks = append(locks, string(l))
        }
    }
    sort.Strings(locks)

    splitLocks := make([]Lock, 0)
    for i := range(locks) {
        if i >= m.rebalanceThreshold / 2 {
            return splitLocks
        }
        splitLocks = append(splitLocks, Lock(locks[i]))
    }
    fmt.Println("MASTER: error in splitting locks")
    return splitLocks
}

func (m *MasterFSM) handleReleasedRecalcitrant(l Lock) func() []byte {
   /* Find replica group to place lock into, remove recalcitrant lock entry in map. */
   m.fsmLock.RLock()
   newReplicaGroup := m.recalcitrantDestMap[l]
   delete(m.recalcitrantDestMap, l)

   sendLockFunc := func() []byte {
       m.fsmLock.RLock()
       fmt.Println("MASTER: send ", l, " to ", newReplicaGroup, " at ", m.clusterMap[newReplicaGroup])
       m.fsmLock.RUnlock()
       m.askWorkerToClaimLocks(newReplicaGroup, []Lock{l})

       /* Tell master to transfer ownership of locks. */
       args := make(map[string]string)
       args[FunctionKey] = TransferRecalCommand
       args[LockArgKey] = string(l)
       args[OldGroupKey] = strconv.Itoa(int(m.lockMap[l]))
       args[NewGroupKey] = strconv.Itoa(int(newReplicaGroup))
       command, json_err := json.Marshal(args)
       if json_err != nil {
           fmt.Println("MASTER: json error")
       }
       return command
   }

   m.fsmLock.RUnlock()
   return sendLockFunc
}
