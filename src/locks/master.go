package locks

import(
    "raft"
    "fmt"
    "strings"
    "io"
    "encoding/json"
    "bytes"
)

type MasterFSM struct {
    /* Map of locks to replica group where stored. */
    lockMap             map[Lock]*masterLockState
    /* Map of replica group IDs to server addresses of the servers in that replica group. */
    clusterMap          map[ReplicaGroupId][]raft.ServerAddress
    /* Map of lock domains to replica group where should be stored. */
    domainPlacementMap  map[Domain][]ReplicaGroupId
    /* Tracks number of locks held by each replica group. */
    numLocksHeld        map[ReplicaGroupId]int
    /* Next replica group ID. */
    nextReplicaGroupId  ReplicaGroupId
}

type masterLockState struct {
    /* Replica group where lock is stored. */
    ReplicaGroup        ReplicaGroupId
    /* True if lock is in transit, false otherwise. */
    InTransit           bool
}

/* Constants for recruiting new clusters. */
var recruitAddrs [][]raft.ServerAddress = [][]raft.ServerAddress{{"127.0.0.1:6000", "127.0.0.1:6001", "127.0.0.1:6002"}}
const numClusterServers = 3

/* TODO: what do we need to do here? */
type MasterSnapshot struct{
    LockMap             map[Lock]*masterLockState
    ClusterMap          map[ReplicaGroupId][]raft.ServerAddress
    DomainPlacementMap  map[Domain][]ReplicaGroupId
    NumLocksHeld        map[ReplicaGroupId]int
    NextReplicaGroupId  ReplicaGroupId
}

func CreateMaster() (*MasterFSM) {
    m := &MasterFSM {
        lockMap:            make(map[Lock]*masterLockState),
        clusterMap:         make(map[ReplicaGroupId][]raft.ServerAddress),
        domainPlacementMap: make(map[Domain][]ReplicaGroupId),
        numLocksHeld:       make(map[ReplicaGroupId]int),
        nextReplicaGroupId: 0,
    }
    err := m.recruitInitialCluster()
    if err != nil {
        //TODO
        fmt.Println(err)
    }
    return m
}

/* TODO: Do we need some kind of FSM init? like a flag that's set for if it's inited and then otherwise we init on first request? */
func (m *MasterFSM) Apply(log *raft.Log) (interface{}, func()) {
    /* Interpret log to find command. Call appropriate function. */

    fmt.Println("MASTER APPLY")
    args := make(map[string]string)
    err := json.Unmarshal(log.Data, &args)
    if err != nil {
        //TODO
        fmt.Println("error in apply")
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
    }



    return nil, nil
}

/* TODO: what to do here? */
func (m *MasterFSM) Snapshot() (raft.FSMSnapshot, error) {
    /* TODO need to lock fsm? */
    s := MasterSnapshot{LockMap: m.lockMap, ClusterMap: m.clusterMap,
         DomainPlacementMap: m.domainPlacementMap, NumLocksHeld: m.numLocksHeld,
         NextReplicaGroupId: m.nextReplicaGroupId}
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
    m.lockMap = snapshotRestored.LockMap
    m.clusterMap = snapshotRestored.ClusterMap
    m.domainPlacementMap = snapshotRestored.DomainPlacementMap
    m.numLocksHeld = snapshotRestored.NumLocksHeld
    m.nextReplicaGroupId = snapshotRestored.NextReplicaGroupId

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

func (m *MasterFSM) createLock(l Lock) (func(), CreateLockResponse) {
    fmt.Println("master creating lock")
    /* Check if already exists (return false).
      Check that intermediate domains exist. 
      Get replica group ID where should be put.
      Tell replica group to make that log
      numLocksHeld[group]++
      Add lock to lockMap */
    if _, ok := m.lockMap[l]; ok {
        return nil, CreateLockResponse{ErrLockExists}
    }
    if len(string(l)) == 0 {
        return nil, CreateLockResponse{ErrEmptyPath}
    }
    domain := getParentDomain(string(l))
    fmt.Println(domain)
    replicaGroups, ok := m.domainPlacementMap[domain]
    if !ok || len(replicaGroups) == 0 {
        fmt.Println("first error")
        return nil, CreateLockResponse{ErrNoIntermediateDomain}
    }
    replicaGroup, err := m.choosePlacement(replicaGroups)
    if err != "" {
        fmt.Println("second error")
        return nil, CreateLockResponse{err}
    }
    m.numLocksHeld[replicaGroup]++
    m.lockMap[l] = &masterLockState{ReplicaGroup: replicaGroup, InTransit: true}
    /* Only do this if r not nil and is leader */
    /* Is there a leader lock I need to acquire around this? make sure don't lose leader mandate? */
    /* Need to make sure replica group has made lock before replying to client. */
    /* TODO: mark not as transit when receive response from worker cluster. */
    /* TODO: deal with casting issues here. Maybe just make new TCP transport for now?? */
    f := func(){
            /* TODO: generate command */
            command := []byte{} 
            raft.SendSingletonRequestToCluster(m.clusterMap[replicaGroup], command, &raft.ClientResponse{})
        }
    fmt.Println("lock creation successfull")
    return f, CreateLockResponse{""}
}

func (m *MasterFSM) createLockDomain(d Domain) CreateDomainResponse {
    fmt.Println("master creating domain")
    /* Check if already exists (return false).
       Check that intermediate domains exist.
       Get replica group ID where should be put.
       Add to domainPlacementMap. */
    if _, ok := m.domainPlacementMap[d]; ok {
       return CreateDomainResponse{ErrDomainExists}
    }
    if len(string(d)) == 0 {
        return CreateDomainResponse{ErrEmptyPath}
    }
    domain := getParentDomain(string(d))
    replicaGroups, ok := m.domainPlacementMap[domain]
    if !ok || len(replicaGroups) == 0 {
        return CreateDomainResponse{ErrNoIntermediateDomain}
    }
    replicaGroup, err := m.choosePlacement(replicaGroups)
    if err != "" {
        return CreateDomainResponse{err}
    }
    m.domainPlacementMap[d] = []ReplicaGroupId{replicaGroup}
    return CreateDomainResponse{""}
}

func (m *MasterFSM) findLock(l Lock) (LocateLockResponse) {
    /* Check that lock exists.
       Check lockMap to find replica group ID. 
       Return replica groupID and servers using clusterMap. */
    lockState, ok := m.lockMap[l]
    if !ok {
        return LocateLockResponse{-1, nil, ErrLockDoesntExist}
    }
    replicaGroup := lockState.ReplicaGroup
    return LocateLockResponse{replicaGroup, m.clusterMap[replicaGroup], ""}
}

func getParentDomain(path string) Domain {
    /* Set root as parent of all directories */
    slice := []string{"/"}
    split := strings.Split(path, "/")
    for _, s := range(split) {
        slice = append(slice, s)
    }
    /* Remove last element. */
    slice = slice[:len(slice)-1]
    return Domain(strings.Join(slice, "/"))
}

func (m *MasterFSM) choosePlacement(replicaGroups []ReplicaGroupId) (ReplicaGroupId, string) {
    if len(replicaGroups) == 0 {
        return -1, ErrNoPlacement
    }
    chosen := replicaGroups[0]
    minLoad := m.numLocksHeld[chosen]
    for _, replicaGroup := range(replicaGroups) {
        if minLoad < m.numLocksHeld[replicaGroup] {
            chosen = replicaGroup
            minLoad = m.numLocksHeld[replicaGroup]
        }
    }
    if chosen == -1 {
        return -1, ErrNoPlacement
    }
    return chosen, "" 
}

func (m *MasterFSM) recruitCluster() (ReplicaGroupId, error) {
    MakeCluster(numClusterServers, CreateWorker(), recruitAddrs[m.nextReplicaGroupId])
    m.clusterMap[m.nextReplicaGroupId] = recruitAddrs[m.nextReplicaGroupId]
    m.numLocksHeld[m.nextReplicaGroupId] = 0
    id := m.nextReplicaGroupId //TODO race condition?
    m.nextReplicaGroupId++
    return id, nil
}

func (m *MasterFSM) recruitInitialCluster() error {
    id, err := m.recruitCluster()
    m.domainPlacementMap["/"] = []ReplicaGroupId{id}
    return err
}
