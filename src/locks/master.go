package locks

import(
    "raft"
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
    RecruitAddrs        []RecruitInfo
    /* Whether to recruit clusters locally - true for testing. */
    RecruitClustersLocally  bool
    /* Threshold at which to split. */
    MaxFreq         float64
    /* Threshold at which to join. */
    MinFreq         float64
    /* Ideal frequency of accesses at worker cluster. */
    IdealFreq       float64
    /* Max number of inactivi periods before join. */
    MaxInactivePeriods  int
    /* Eventual destinations of recalcitrant locks. */
    RecalcitrantDestMap map[Lock]ReplicaGroupId
    /* Rebalances in progress */
    RebalancingInProgress map[ReplicaGroupId]bool
    /* Map of lock to average frequency accessed. Maintained with updates from worker. */
    LockFreqStatsMap          map[Lock]FreqStats
    /* Map of replica group to average frequency accessed. */
    GroupFreqStatsMap       map[ReplicaGroupId]FreqStats
    /* Map of worker sessions. */
    WorkerSessionMap        map[ReplicaGroupId]*raft.Session
    SessionLock             sync.RWMutex
    Trans                   *raft.NetworkTransport
}

type FreqStats struct {
    /* Exponentially weighted moving average of frequency of accesses over period. */
    avgFreq        float64
    /* Time last updated. */
    lastUpdate  time.Time

}

type RecruitInfo struct {
    addrs                   []raft.ServerAddress
    shouldRecruitLocally    bool
}

var PERIOD time.Duration = 200 * time.Millisecond

const WEIGHT = 0.2

const STABILIZE_FACTOR = 1000000 

const IDEAL_FACTOR = 1 

const numClusterServers = 3

const NO_WORKER = ReplicaGroupId(-1)

type MasterSnapshot struct{
    json    []byte
}

func CreateMasters (n int, clusterAddrs []raft.ServerAddress, recruitList [][]raft.ServerAddress, maxFreq float64, minFreq float64, idealFreq float64, maxInactivePeriods int, recruitClustersLocally bool, transports []*raft.NetworkTransport) ([]raft.FSM) {
    masters := make([]*MasterFSM, n)
    for i := range(masters) {
        masters[i] = &MasterFSM {
            LockMap:            make(map[Lock]ReplicaGroupId),
            ClusterMap:         make(map[ReplicaGroupId][]raft.ServerAddress),
            DomainPlacementMap: make(map[Domain][]ReplicaGroupId),
            NumLocksHeld:       make(map[ReplicaGroupId]int),
            NextReplicaGroupId: 0,
            MasterCluster:      clusterAddrs,
            RecruitAddrs:       make([]RecruitInfo, 0),
            RecruitClustersLocally: recruitClustersLocally,
            MaxFreq:            maxFreq,
            MinFreq:            minFreq,
            IdealFreq:          idealFreq,
            MaxInactivePeriods: maxInactivePeriods,
            RecalcitrantDestMap: make(map[Lock]ReplicaGroupId),
            RebalancingInProgress: make(map[ReplicaGroupId]bool),
            LockFreqStatsMap:         make(map[Lock]FreqStats),
            GroupFreqStatsMap:      make(map[ReplicaGroupId]FreqStats),
            WorkerSessionMap:       make(map[ReplicaGroupId]*raft.Session),
            Trans:                  transports[i],
        }
        for _,addrs := range recruitList {
            elem := RecruitInfo{addrs: addrs, shouldRecruitLocally: recruitClustersLocally}
            masters[i].RecruitAddrs = append(masters[i].RecruitAddrs, elem)
        }
    }
    if n <= 0 {
        //fmt.Println("MASTER: Cannot have number of masters <= 0")
    }
    err := recruitInitialCluster(masters, recruitList[0], recruitClustersLocally)
    if err != nil {
        //TODO
        //fmt.Println(err)
    }
    fsms := make([]raft.FSM, n)
    for i, m := range(masters) {
        fsms[i] = m
    }

    return fsms
}

func (m *MasterFSM) Apply(log *raft.Log) (interface{}, []func() [][]byte) {
    /* Interpret log to find command. Call appropriate function. */

    args := make(map[string]string)
    err := json.Unmarshal(log.Data, &args)
    if err != nil {
        //fmt.Println("MASTER: error in apply: ", err)
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
            return response, []func()[][]byte{}
        case LocateLockCommand:
            l := Lock(args[LockArgKey])
            response := m.findLock(l)
            return response, []func()[][]byte{}
        case ReleasedRecalcitrantCommand:
            l := Lock(args[LockArgKey])
            callback := m.handleReleasedRecalcitrant(l)
            return nil, callback
        case DeleteLockNotAcquiredCommand:
            l := Lock(args[LockArgKey])
            m.deleteLockNotAcquired(l)
            return nil, []func()[][]byte{}
        case DeleteRecalLockCommand:
            l := Lock(args[LockArgKey])
            m.markLockForDeletion(l)
            return nil, []func()[][]byte{}
        case FrequencyUpdateCommand:
            lockArr := string_to_lock_array(args[LockArrayKey])
            countArr := string_to_int_array(args[CountArrayKey])
            callback := m.updateFrequencies(lockArr, countArr)
            return nil, callback
        case TransferLockGroupCommand:
            oldGroup, err1 := strconv.Atoi(args[OldGroupKey])
            newGroup, err2 := strconv.Atoi(args[NewGroupKey])
            if err1 != nil || err2 != nil {
                //fmt.Println("MASTER: group IDs can't be converted to ints")
                return nil, []func()[][]byte{}
            }
            lockArr := string_to_lock_array(args[LockArrayKey])
            recalArr := string_to_lock_array(args[LockArray2Key])
            callback := m.initialLockGroupTransfer(ReplicaGroupId(oldGroup), ReplicaGroupId(newGroup), lockArr, recalArr)
            return nil, callback
        case TransferRecalCommand:
            oldGroup, err1 := strconv.Atoi(args[OldGroupKey])
            newGroup, err2 := strconv.Atoi(args[NewGroupKey])
            if err1 != nil || err2 != nil {
                //fmt.Println("MASTER: group IDs can't be converted to ints")
                return nil, []func()[][]byte{}
            }
            l := Lock(args[LockArgKey])
            callback := m.singleRecalcitrantLockTransfer(ReplicaGroupId(oldGroup), ReplicaGroupId(newGroup), l)
            return nil, callback
        }

    return nil, []func()[][]byte{}
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
    m.MaxFreq = snapshotRestored.MaxFreq
    m.MinFreq = snapshotRestored.MinFreq
    m.IdealFreq = snapshotRestored.IdealFreq
    m.MaxInactivePeriods = snapshotRestored.MaxInactivePeriods
    m.RecalcitrantDestMap = snapshotRestored.RecalcitrantDestMap
    m.LockFreqStatsMap = snapshotRestored.LockFreqStatsMap
    m.GroupFreqStatsMap = snapshotRestored.GroupFreqStatsMap
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

func (m *MasterFSM) createLock(l Lock) ([]func() [][]byte, CreateLockResponse) {
    m.FsmLock.Lock()
    defer m.FsmLock.Unlock()
    //fmt.Println("MASTER: master creating lock with name ", string(l))
    if len(string(l)) == 0 {
        return []func() [][]byte{}, CreateLockResponse{ErrEmptyPath}
    }
    domain := getParentDomain(string(l))
    replicaGroups, ok := m.DomainPlacementMap[domain]
    if !ok || len(replicaGroups) == 0 {
        return []func() [][]byte{}, CreateLockResponse{ErrNoIntermediateDomain}
    }
    if _, ok := m.LockMap[l]; ok {
        return []func() [][]byte{}, CreateLockResponse{ErrLockExists}
    }
    replicaGroup, err := m.choosePlacement(replicaGroups)
    if err != "" {
        return []func() [][]byte{}, CreateLockResponse{err}
    }
    //fmt.Println("MASTER: put lock ", string(l), " in domain", string(domain))
    m.NumLocksHeld[replicaGroup]++
    m.LockMap[l] = replicaGroup
    m.LockFreqStatsMap[l] = FreqStats{lastUpdate: time.Now(), avgFreq: 1}

    /* Trigger rebalancing if number of locks held by replica group >= rebalance threshold. */
    rebalanceCallbacks := m.loadBalanceCheck()

    f := func() [][]byte {
            m.askWorkerToClaimLocks(replicaGroup, []Lock{l})
            var commands [][]byte
            for _, callback := range rebalanceCallbacks {
                commands = callback()
                for _,command := range commands {
                    commands = append(commands, command)
                }
            }
            return commands
    }
    //callbacks := append(rebalanceCallbacks, f)

    return /*callbacks*/[]func()[][]byte{f}, CreateLockResponse{Success}
}

func (m *MasterFSM) loadBalanceCheck() []func()[][]byte {
    /* Trigger rebalancing if number of locks held by replica group >= rebalance threshold. */
    var rebalanceCallbacks []func() [][]byte = nil
    overworkedWorker := m.findOverworkedWorker()
    if overworkedWorker != NO_WORKER {
        rebalanceCallbacks = m.shedLoad(overworkedWorker)
    } else {
        underworkedWorker := m.findUnderworkedWorker()
        if underworkedWorker != NO_WORKER {
            rebalanceCallbacks = m.retireWorker(underworkedWorker)
        }
    }
    return rebalanceCallbacks
}

func (m *MasterFSM) deleteLock(l Lock) ([]func() [][]byte, DeleteLockResponse) {
    m.FsmLock.Lock()
    defer m.FsmLock.Unlock()
    //fmt.Println("MASTER: master deleting lock ", string(l))
    replicaGroup, ok := m.LockMap[l]
    if !ok {
        return []func() [][]byte{}, DeleteLockResponse{ErrLockDoesntExist}
    }
    triggerDelete := func()[][]byte {
        //fmt.Println("MASTER: delete lock " + l)
        delete_func := func() [][]byte {
            recalcitrantLocks := m.initiateTransfer(replicaGroup, []Lock{l})
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
                //fmt.Println("MASTER: json error")
            }
            return [][]byte{command}
        }
        return delete_func()
    }
    return []func() [][]byte{triggerDelete}, DeleteLockResponse{Success}
    // TODO: call for rebalance here, make rebalance more generic to join or split
    // if not using a cluster any more, need to make sure don't continue sending locks there (cluster is "retiring")
}

func (m *MasterFSM) createLockDomain(d Domain) CreateDomainResponse {
    m.FsmLock.Lock()
    defer m.FsmLock.Unlock()
    //fmt.Println("MASTER: master creating domain ", string(d))
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
    minLoad := m.GroupFreqStatsMap[chosen].avgFreq
    for _, replicaGroup := range(replicaGroups) {
        if minLoad > m.GroupFreqStatsMap[chosen].avgFreq {
            chosen = replicaGroup
            minLoad = m.GroupFreqStatsMap[replicaGroup].avgFreq
        }
    }
    return chosen, "" 
}

func recruitInitialCluster(masters []*MasterFSM, workerAddrs []raft.ServerAddress, recruit_locally bool) (error) {
    if recruit_locally {
        transports := make([]*raft.NetworkTransport, len(workerAddrs))
        for i := range workerAddrs {
            trans,_ := raft.NewTCPTransport(string(workerAddrs[i]), nil, 2, time.Second, nil)
            transports[i] = trans
        }
        MakeCluster(numClusterServers, CreateWorkers(len(workerAddrs), masters[0].MasterCluster, workerAddrs, transports), workerAddrs, transports)
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

func (m *MasterFSM) deleteLockNotAcquired(l Lock) []func() [][]byte {
    m.FsmLock.Lock()
    defer m.FsmLock.Unlock()
    replicaGroup := m.LockMap[l]
    delete(m.LockMap, l)
    delete(m.LockFreqStatsMap, l)
    m.NumLocksHeld[replicaGroup]--

    /* Check if should split or join */
    rebalanceCallbacks := m.loadBalanceCheck()

    f := func() [][]byte {
        m.askWorkerToDisownLocks(replicaGroup, []Lock{l})
        var commandList [][]byte
        for _,rebalanceCallback := range rebalanceCallbacks {
            commands := rebalanceCallback()
            for _,command := range commands {
                commandList = append(commandList, command)
            }
        }
        return commandList 
    }

    //callbacks := append(rebalanceCallbacks, f)

    return []func()[][]byte{f}//callbacks
}

func (m* MasterFSM) markLockForDeletion(l Lock) {
    m.FsmLock.Lock()
    defer m.FsmLock.Unlock()
    m.RecalcitrantDestMap[l] = -1
}

func (m *MasterFSM) shedLoad(replicaGroup ReplicaGroupId) []func() [][]byte {
    locksToMove := m.getLocksToShedLoadToIdeal(replicaGroup)
    redistributeCallbacks := m.tryRedistributeLocks(replicaGroup, locksToMove)
    if redistributeCallbacks != nil {
        return redistributeCallbacks
    }
    locksToMove = m.getLocksToSplitEvenly(replicaGroup)
    if (int(m.NextReplicaGroupId) < len(m.RecruitAddrs)) {
        return m.splitToNewWorker(replicaGroup, locksToMove)
    }
    return []func()[][]byte{}
}

func (m *MasterFSM) tryRedistributeLocks(replicaGroup ReplicaGroupId, locksToRedistribute []Lock) []func() [][]byte {
    // TODO: in future, split between multiple clusters, don't just dump on 1 random cluster

    redistributeMap := make(map[ReplicaGroupId][]Lock)
    anticipatedAdditionalLoad := make(map[ReplicaGroupId]float64)
    for _, l := range locksToRedistribute {
        newReplicaGroup := m.getWorkerToTakeLock(l, replicaGroup, anticipatedAdditionalLoad)
        if newReplicaGroup == NO_WORKER {
            /* abort if no other worker to join with. */
            return nil 
        }
        if _,ok := redistributeMap[newReplicaGroup]; !ok {
            redistributeMap[newReplicaGroup] = make([]Lock, 0)
        }
        anticipatedAdditionalLoad[newReplicaGroup] += m.LockFreqStatsMap[l].avgFreq
        redistributeMap[newReplicaGroup] = append(redistributeMap[newReplicaGroup], l)
    }

    rebalancingFuncs := make([]func()[][]byte, 0)

    //fmt.Println("redistributing locks for: ", replicaGroup)
    m.RebalancingInProgress[replicaGroup] = true


    for newReplicaGroup := range redistributeMap {
        locksToMove := redistributeMap[newReplicaGroup]
        m.patchReferencesToOldWorker(replicaGroup, newReplicaGroup, locksToMove)

        rebalancingFunc := func() [][]byte {
            /* Initiate rebalancing and find recalcitrant locks. */
            recalcitrantLocks := m.initiateTransfer(replicaGroup, locksToMove)

            recalcitrantLocksList := make([]Lock, 0)
            for l := range(recalcitrantLocks) {
                recalcitrantLocksList = append(recalcitrantLocksList, l)
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
                //fmt.Println("MASTER: json error")
            }
            return [][]byte{command}
        }

        rebalancingFuncs = append(rebalancingFuncs, rebalancingFunc)

    }
    return rebalancingFuncs 
}

func (m *MasterFSM) splitToNewWorker(replicaGroup ReplicaGroupId, locksToMove []Lock) ([]func() [][]byte) {
    //fmt.Println("MASTER: SPLITTING LOAD")
    //fmt.Println("is rebalancing in progress for ", replicaGroup, ": ", m.RebalancingInProgress[replicaGroup])
    /* Update state in preparation for adding new cluster. */
    //fmt.Println("next replica group id: ", m.NextReplicaGroupId)
    workerAddrs := m.RecruitAddrs[m.NextReplicaGroupId].addrs
    shouldMakeNewCluster := m.RecruitAddrs[m.NextReplicaGroupId].shouldRecruitLocally
    newReplicaGroup := m.NextReplicaGroupId
    m.ClusterMap[newReplicaGroup] = workerAddrs
    m.NumLocksHeld[newReplicaGroup] = 0
    m.NextReplicaGroupId++
    m.RebalancingInProgress[replicaGroup] = true
    rebalancing_func := func() [][]byte {
        /* Initiate rebalancing and find recalcitrant locks. */
        recalcitrantLocks := m.initiateTransfer(replicaGroup, locksToMove)

        recalcitrantLocksList := make([]Lock, 0)
        for l := range(recalcitrantLocks) {
            recalcitrantLocksList = append(recalcitrantLocksList, l)
        }

        /* Recruit new replica group to store rebalanced locks. */
        //fmt.Println("recruit addr list = ", m.RecruitAddrs, ", next replica group id (after ++) = ", m.NextReplicaGroupId)
        if (shouldMakeNewCluster) {
            transports := make([]*raft.NetworkTransport, len(workerAddrs))
                for i := range workerAddrs {
                    trans,_ := raft.NewTCPTransport(string(workerAddrs[i]), nil, 2, time.Second, nil)
                    transports[i] = trans
                }
            MakeCluster(numClusterServers, CreateWorkers(len(workerAddrs), m.MasterCluster, workerAddrs, transports), workerAddrs, transports)
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
            //fmt.Println("MASTER: json error")
        }
        return [][]byte{command}
    }
    //fmt.Println("afterwards....")

    return []func() [][]byte{rebalancing_func}
}

// can return NO_WORKER if not a good candidate
func (m *MasterFSM) getWorkerToTakeLock(l Lock, replicaGroup ReplicaGroupId, anticipatedAdditionalLoad map[ReplicaGroupId]float64) ReplicaGroupId {
    lockFreq := m.LockFreqStatsMap[l].avgFreq
    candidates := make(map[ReplicaGroupId]bool)
    d := getParentDomain(string(l))
    domainGroups := m.DomainPlacementMap[d]
    for _, domainGroup := range domainGroups {
        if domainGroup != replicaGroup && !m.RebalancingInProgress[domainGroup] && (m.GroupFreqStatsMap[domainGroup].avgFreq + anticipatedAdditionalLoad[domainGroup] + lockFreq) < m.calcMaxFreq() {
            candidates[domainGroup] = true
        }
    }
    candidateList := make([]ReplicaGroupId, 0)
    for candidate := range candidates {
        candidateList = append(candidateList, candidate)
    }
    if len(candidateList) == 0 {
        //fmt.Println("No viable worker to transfer locks to.")
        return NO_WORKER
    }
    newReplicaGroup,_ := m.choosePlacement(candidateList)
    return newReplicaGroup
}

func (m *MasterFSM) patchReferencesToOldWorker(oldReplicaGroup ReplicaGroupId, newReplicaGroup ReplicaGroupId, locksToMove []Lock) {
    remainingDomains := make(map[Domain]bool)
    locksToMoveMap := make(map[Lock]bool)
    for _,l := range locksToMove {
        locksToMoveMap[l] = true
    }
    for l := range m.LockMap {
        if m.LockMap[l] == oldReplicaGroup && !locksToMoveMap[l] {
            d := getParentDomain(string(l))
            remainingDomains[d] = true
        }
    }
    // Update domain placement map.
    for domain, groups := range(m.DomainPlacementMap) {
        if remainingDomains[domain] {
            continue
        }
        shouldAppend := false
        for i := 0; i < len(groups); i++ {
            //fmt.Println("i=", i, ", groups=", groups)
            if groups[i] == oldReplicaGroup || groups[i] == newReplicaGroup {
                if i == len(groups) - 1 {
                    groups = groups[:i]
                } else {
                    groups = append(groups[:i], groups[i+1:]...)
                }
                i++
                shouldAppend = true
            }
        }
        if shouldAppend {
            groups = append(groups, newReplicaGroup)
            m.DomainPlacementMap[domain] = groups
        }
    }
    // Update recalcitrant lock destination map. 
    for l, dest := range(m.RecalcitrantDestMap) {
        if dest == oldReplicaGroup && !remainingDomains[getParentDomain(string(l))] {
            m.RecalcitrantDestMap[l] = newReplicaGroup
        }
    }
    // Update group freq stats map
    newGroupFreq := m.GroupFreqStatsMap[newReplicaGroup]
    oldGroupFreq := m.GroupFreqStatsMap[oldReplicaGroup]
    for _,l := range locksToMove {
        newGroupFreq.avgFreq += m.LockFreqStatsMap[l].avgFreq
        oldGroupFreq.avgFreq -= m.LockFreqStatsMap[l].avgFreq
    }
    m.GroupFreqStatsMap[newReplicaGroup] = newGroupFreq
    m.GroupFreqStatsMap[oldReplicaGroup] = oldGroupFreq
}

func (m *MasterFSM) retireWorker(replicaGroup ReplicaGroupId) []func() [][]byte {
    //fmt.Println("*** checking to retire worker")
    locksToMove := m.getLocksToConsolidate(replicaGroup)
    retireCallbacks := m.tryRedistributeLocks(replicaGroup, locksToMove)
    if retireCallbacks != nil {
        //fmt.Println("****RETIRING WORKER: ", replicaGroup)
        return retireCallbacks
    }
    return []func() [][]byte{}
}

func (m *MasterFSM) checkForFullyRetiredWorker(replicaGroup ReplicaGroupId) {
    if m.NumLocksHeld[replicaGroup] == 0 {
        serverAddrs := m.ClusterMap[replicaGroup]
        // leave old cluster map entry to allow for some cleanup
        //delete(m.NumLocksHeld, replicaGroup)
        //delete(m.RebalancingInProgress, replicaGroup)
        //delete(m.GroupFreqStatsMap, replicaGroup)
        m.RebalancingInProgress[replicaGroup] = true
        m.RecruitAddrs = append(m.RecruitAddrs, RecruitInfo{addrs: serverAddrs, shouldRecruitLocally: false})
    }
}


func (m *MasterFSM) genericClusterRequest(replicaGroup ReplicaGroupId, args map[string]string, resp *raft.ClientResponse) {
    command, json_err := json.Marshal(args)
    if json_err != nil {
        //fmt.Println("MASTER: JSON ERROR")
    }
    m.FsmLock.RLock()
    m.SessionLock.Lock()
    defer m.SessionLock.Unlock()
    // TODO: USE SESSION
    session, ok := m.WorkerSessionMap[replicaGroup]
    var err error = nil
    if !ok {
        session, err = raft.CreateClientSession(m.Trans, m.ClusterMap[replicaGroup], nil)
        m.WorkerSessionMap[replicaGroup] = session
    }
    if err != nil {
        return
    }
    send_err := session.SendRequest(command, resp)
    m.FsmLock.RUnlock()
    if send_err != nil {
       //fmt.Println("MASTER: send err ", send_err)
    }
}

func (m *MasterFSM) initiateTransfer(replicaGroup ReplicaGroupId, locksToMove []Lock) map[Lock]int {
    /* Send RPC to worker with locks_to_move */
    args := make(map[string]string)
    args[FunctionKey] = TransferCommand
    args[LockArrayKey] = lock_array_to_string(locksToMove)
    resp := raft.ClientResponse{}
    m.genericClusterRequest(replicaGroup, args, &resp)
    var response TransferResponse
    unmarshal_err := json.Unmarshal(resp.ResponseData, &response)
    if unmarshal_err != nil {
        //fmt.Println("MASTER: error unmarshalling")
    }
    return response.RecalcitrantLocks
}

func (m *MasterFSM) calcMaxFreq() float64 {
    if (int(m.NextReplicaGroupId) == len(m.RecruitAddrs)) {
        return STABILIZE_FACTOR * m.getTotalFreq() / float64(len(m.ClusterMap))
    }
    return m.MaxFreq
}

func (m *MasterFSM) findOverworkedWorker() ReplicaGroupId {
    for replicaGroup := range m.NumLocksHeld/*m.GroupFreqStatsMap*/ {
        // TODO: replace with real tracking
        //fmt.Println("check rep group ",  replicaGroup)
        if m.GroupFreqStatsMap[replicaGroup].avgFreq >= m.calcMaxFreq() {
            //fmt.Println("overthreshold but rebalancing")
            if _, ok := m.RebalancingInProgress[replicaGroup]; !ok {
                //fmt.Println("found overworked: ", replicaGroup)
                return replicaGroup
            }
        }
    }
    return NO_WORKER
}

func (m *MasterFSM) findUnderworkedWorker() ReplicaGroupId {
    for replicaGroup := range m.NumLocksHeld/*m.GroupFreqStatsMap*/ {
        // TODO: replace with real tracking
        if m.GroupFreqStatsMap[replicaGroup].avgFreq <= m.MinFreq && time.Since(m.GroupFreqStatsMap[replicaGroup].lastUpdate) / PERIOD >= time.Duration(m.MaxInactivePeriods) {
            //fmt.Println("num locks held = ", m.NumLocksHeld[replicaGroup])
            if _, ok := m.RebalancingInProgress[replicaGroup]; !ok {
                //fmt.Println("reporting underworked at ", replicaGroup)
                return replicaGroup
            }
        }
    }
    return NO_WORKER
}

func (m *MasterFSM) getTotalFreq() float64 {
    total := 0.0
    for _, freq := range m.GroupFreqStatsMap {
        total += freq.avgFreq
    }
    return total
}



func (m *MasterFSM) askWorkerToClaimLocks(replicaGroup ReplicaGroupId, movingLocks []Lock) {
    /* Send RPC to worker with locks to claim. */
    //fmt.Println("MASTER: ask worker to claim locks\n")
    args := make(map[string]string)
    args[FunctionKey] = ClaimLocksCommand
    args[LockArrayKey] = lock_array_to_string(movingLocks)
    m.genericClusterRequest(replicaGroup, args, &raft.ClientResponse{})
    // TODO: do we need the claimed locks anywhere?
}

func (m *MasterFSM) askWorkerToDisownLocks(replicaGroup ReplicaGroupId, movingLocks []Lock) {
    /* Send RPC to worker with locks to claim. */
    //fmt.Println("MASTER: ask worker to disown locks")
    args := make(map[string]string)
    args[FunctionKey] = DisownLocksCommand
    args[LockArrayKey] = lock_array_to_string(movingLocks)
    m.genericClusterRequest(replicaGroup, args, &raft.ClientResponse{}) 
}

/* Transfer ownership of locks in master and tell old replica group to disown locks. Should only be called after new replica group owns locks. */
func (m *MasterFSM) initialLockGroupTransfer(oldGroupId ReplicaGroupId, newGroupId ReplicaGroupId, movingLocks []Lock, recalcitrantLocks []Lock) []func() [][]byte {
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
        //fmt.Println("DONE REBAL FOR ", oldGroupId)
        delete(m.RebalancingInProgress, oldGroupId)
    }

    /* Ask old replica group to disown locks being moved. */
    f := func() [][]byte {
        m.askWorkerToDisownLocks(oldGroupId, movingLocks)
        return [][]byte{}
    }
    return []func() [][]byte{f}
}

func (m *MasterFSM) singleRecalcitrantLockTransfer(oldGroupId ReplicaGroupId, newGroupId ReplicaGroupId, l Lock) []func() [][]byte {
    m.FsmLock.Lock()
    m.FsmLock.Unlock()
    /* Update state for transferring recalcitrant lock. */
    m.LockMap[l] = newGroupId
    m.NumLocksHeld[newGroupId]++
    m.NumLocksHeld[oldGroupId]--
    m.checkForFullyRetiredWorker(oldGroupId)

    /* Ask worker to disown lock now that transferred. */
    f := func() [][]byte {
        m.askWorkerToDisownLocks(oldGroupId, []Lock{l})
        return [][]byte{}
    }

    return []func() [][]byte{f}
}

// Lexicographically split evenly based on frequency of access
func (m *MasterFSM) getLocksToSplitEvenly(replicaGroup ReplicaGroupId) ([]Lock) {
    /* Find all domains for locks in replica group. */
    locks := make([]string, 0)
    for l := range(m.LockMap) {
        if m.LockMap[l] == replicaGroup {
            locks = append(locks, string(l))
        }
    }
    sort.Strings(locks)

    totFreq := m.GroupFreqStatsMap[replicaGroup].avgFreq
    currSplitFreq := 0.0
    splitLocks := make([]Lock, 0)
    for _,l := range(locks) {
        if currSplitFreq >= totFreq / 2.0 {
            return splitLocks
        }
        currSplitFreq += m.LockFreqStatsMap[Lock(l)].avgFreq
        splitLocks = append(splitLocks, Lock(l))
    }
    //fmt.Println("MASTER: error in splitting locks")
    return splitLocks
}

func (m *MasterFSM) getLocksToShedLoadToIdeal(replicaGroup ReplicaGroupId) ([]Lock) {
    /* Find all domains for locks in replica group. */
    locks := make([]string, 0)
    for l := range(m.LockMap) {
        if m.LockMap[l] == replicaGroup {
            locks = append(locks, string(l))
        }
    }
    sort.Strings(locks)

    idealFreq := m.IdealFreq
    if (int(m.NextReplicaGroupId) == len(m.RecruitAddrs)) {
        idealFreq = (m.getTotalFreq() / float64(len(m.ClusterMap))) * (IDEAL_FACTOR);
    }

    goalFreq := m.GroupFreqStatsMap[replicaGroup].avgFreq - idealFreq
    currSplitFreq := 0.0
    splitLocks := make([]Lock, 0)
    for _,l := range(locks) {
        if currSplitFreq >= goalFreq {
            return splitLocks
        }
        currSplitFreq += m.LockFreqStatsMap[Lock(l)].avgFreq
        splitLocks = append(splitLocks, Lock(l))
    }
    //fmt.Println("MASTER: error in splitting locks")
    return splitLocks
}

func (m * MasterFSM) getLocksToConsolidate(replicaGroup ReplicaGroupId) ([]Lock) {
    locks := make([]Lock, 0)
    for l := range(m.LockMap) {
        if m.LockMap[l] == replicaGroup {
            locks = append(locks, l)
        }
    }
    return locks
}

func (m *MasterFSM) handleReleasedRecalcitrant(l Lock) []func() [][]byte {
   /* Find replica group to place lock into, remove recalcitrant lock entry in map. */
   m.FsmLock.Lock()
   defer m.FsmLock.Unlock()
   //m.FsmLock.RLock()
   newReplicaGroup := m.RecalcitrantDestMap[l]
   delete(m.RecalcitrantDestMap, l)
   /* Check if should delete lock. */
   if newReplicaGroup == NO_WORKER {
        replicaGroup := m.LockMap[l]
        m.NumLocksHeld[replicaGroup]--
        m.checkForFullyRetiredWorker(replicaGroup)
        delete(m.LockMap, l)
        delete(m.LockFreqStatsMap, l)
        f := func() [][]byte {
            m.askWorkerToDisownLocks(replicaGroup, []Lock{l})
            return [][]byte{}
        }
        return []func() [][]byte{f}
   }
   //m.FsmLock.RUnlock()

   sendLockFunc := func() [][]byte {
       m.FsmLock.RLock()
       //fmt.Println("MASTER: send ", l, " to ", newReplicaGroup, " at ", m.ClusterMap[newReplicaGroup])
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
           //fmt.Println("MASTER: json error")
       }
       return [][]byte{command}
   }

   return []func() [][]byte{sendLockFunc}
}

func (m *MasterFSM) updateFrequencies(lockArr []Lock, countArr []int)[]func()[][]byte {
    m.FsmLock.Lock()
    defer m.FsmLock.Unlock()
    currTime := time.Now()
    //fmt.Println("Update freqs")
    sumFreq := 0.0
    for i := range lockArr {
        /* Calculate new exponentially weighted moving average. */
        if m.RebalancingInProgress[m.LockMap[lockArr[i]]] {
            newStats := FreqStats{avgFreq: m.LockFreqStatsMap[lockArr[i]].avgFreq, lastUpdate: currTime}
            m.LockFreqStatsMap[lockArr[i]] = newStats
            continue
        }
        stats := m.LockFreqStatsMap[lockArr[i]]
        numPeriodsElapsed := float64(time.Since(stats.lastUpdate) / PERIOD)
        newFreq := float64(countArr[i]) / numPeriodsElapsed
        currAvgFreq := stats.avgFreq
        for i := 0; i < int(numPeriodsElapsed); i++ {
            currAvgFreq = (newFreq * WEIGHT * numPeriodsElapsed) + (currAvgFreq * (1 - WEIGHT))
        }
        //fmt.Println("frequency of ", lockArr[i], " is ", currAvgFreq)
        sumFreq += currAvgFreq
        newStats := FreqStats{avgFreq: currAvgFreq, lastUpdate: currTime}
        m.LockFreqStatsMap[lockArr[i]] = newStats
    }
    // assume update from single worker
    if len(lockArr) > 0 {
        m.GroupFreqStatsMap[m.LockMap[lockArr[0]]] = FreqStats{avgFreq: sumFreq, lastUpdate: currTime}
    }
    //fmt.Println("Returned from update freqs")
    return m.loadBalanceCheck()
}
