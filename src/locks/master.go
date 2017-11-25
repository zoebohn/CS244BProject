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

/* TODO: Do we need some kind of FSM init? like a flag that's set for if it's inited and then otherwise we init on first request? */
func (m *MasterFSM) Apply(log *raft.Log) (interface{}, func()) {
    /* Interpret log to find command. Call appropriate function. */

    args := make(map[string]string)
    err := json.Unmarshal(log.Data, args)
    if err != nil {
        //TODO
    }
    function := args[FunctionKey]
    switch function {
        case CreateLockCommand:
            l := Lock(args[LockArgKey])
            m.createLock(l)
        case CreateDomainCommand:
            d := Domain(args[DomainArgKey])
            m.createLockDomain(d)
        case LocateLockCommand:
            l := Lock(args[LockArgKey])
            m.findLock(l)
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

func (m *MasterFSM) createLock(l Lock) (func(), error) {
    fmt.Println("master creating lock")
    /* Check if already exists (return false).
      Check that intermediate domains exist. 
      Get replica group ID where should be put.
      Tell replica group to make that log
      numLocksHeld[group]++
      Add lock to lockMap */
    if _, ok := m.lockMap[l]; ok {
        return nil, ErrLockExists
    }
    if len(string(l)) == 0 {
        return nil, ErrEmptyPath
    }
    domain := getParentDomain(string(l))
    replicaGroups, ok := m.domainPlacementMap[domain]
    if !ok || len(replicaGroups) == 0 {
        return nil, ErrNoIntermediateDomain
    }
    replicaGroup, err := m.choosePlacement(replicaGroups)
    if err != nil {
        return nil, err
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
    return f, nil
}

func (m *MasterFSM) createLockDomain(d Domain) error {
    fmt.Println("master creating domain")
    /* Check if already exists (return false).
       Check that intermediate domains exist.
       Get replica group ID where should be put.
       Add to domainPlacementMap. */
    if _, ok := m.domainPlacementMap[d]; ok {
       return ErrDomainExists
    }
    if len(string(d)) == 0 {
        return ErrEmptyPath
    }
    domain := getParentDomain(string(d))
    replicaGroups, ok := m.domainPlacementMap[domain]
    if !ok || len(replicaGroups) == 0 {
        return ErrNoIntermediateDomain
    }
    replicaGroup, err := m.choosePlacement(replicaGroups)
    if err != nil {
        return err
    }
    m.domainPlacementMap[d] = []ReplicaGroupId{replicaGroup}
    return nil
}

func (m *MasterFSM) findLock(l Lock) (ReplicaGroupId, []raft.ServerAddress, error) {
    /* Check that lock exists.
       Check lockMap to find replica group ID. 
       Return replica groupID and servers using clusterMap. */
    lockState, ok := m.lockMap[l]
    if !ok {
        return -1, nil, ErrLockDoesntExist
    }
    replicaGroup := lockState.ReplicaGroup
    return replicaGroup, m.clusterMap[replicaGroup], nil
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

func (m *MasterFSM) choosePlacement(replicaGroups []ReplicaGroupId) (ReplicaGroupId, error) {
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
    return chosen, nil
}

func (m *MasterFSM) recruitCluster() error {
    MakeCluster(numClusterServers, &WorkerFSM{}, recruitAddrs[m.nextReplicaGroupId])
    m.clusterMap[m.nextReplicaGroupId] = recruitAddrs[m.nextReplicaGroupId]
    m.numLocksHeld[m.nextReplicaGroupId] = 0
    m.nextReplicaGroupId++
    return nil
}
