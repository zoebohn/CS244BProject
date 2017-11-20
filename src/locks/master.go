package locks

import(
    "raft"
    "strings"
    "io"
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
    replicaGroup        ReplicaGroupId
    /* True if lock is in transit, false otherwise. */
    inTransit           bool
}

/* Constants for recruiting new clusters. */
var recruitAddrs [][]raft.ServerAddress = [][]raft.ServerAddress{{"127.0.0.1:6000", "127.0.0.1:6001", "127.0.0.1:6002"}}
const numClusterServers = 3

/* TODO: what do we need to do here? */
type masterSnapshot struct{}

/* TODO: Do we need some kind of FSM init? like a flag that's set for if it's inited and then otherwise we init on first request? */
func (m *MasterFSM) Apply(log *raft.Log) interface{} { 
    /* Interpret log to find command. Call appropriate function. */
    return nil
}

/* TODO: what to do here? */
func (m *MasterFSM) Snapshot() (raft.FSMSnapshot, error) {
    return &masterSnapshot{}, nil
}

func (m *MasterFSM) Restore(i io.ReadCloser) error {
    return nil
}

func (s *masterSnapshot) Persist(sink raft.SnapshotSink) error {
    return nil
}

func (s *masterSnapshot) Release() {
}

func (m *MasterFSM) createLock(l Lock) error {
   /* Check if already exists (return false).
      Check that intermediate domains exist. 
      Get replica group ID where should be put.
      Tell replica group to make that log
      numLocksHeld[group]++
      Add lock to lockMap */
      if _, ok := m.lockMap[l]; ok {
          return ErrLockExists
      }
      if len(string(l)) == 0 {
          return ErrEmptyPath
      }
      domain := getParentDomain(string(l))
      replicaGroups, ok := m.domainPlacementMap[domain]
      if !ok || len(replicaGroups) == 0 {
          return ErrNoIntermediateDomain
      }
      replicaGroup, err := m.choosePlacement(replicaGroups)
      if err != nil {
          return err
      }
      m.numLocksHeld[replicaGroup]++
      m.lockMap[l] = &masterLockState{replicaGroup: replicaGroup, inTransit: true}
      /* Need to make sure replica group has made lock before replying to client. */
      /* TODO: only leader should send fire and forget RPC, shouldn't affect state */
/*      go func(){
          /* TODO: generate command */
/*        command := []byte{} 
        err := raft.SendSingletonRequestToCluster(m.trans, m.clusterMap[replicaGroup], command, &raft.ClientResponse{})
        if err != nil /* and response is correct *//* {
            m.lockMap[l].inTransit = false
        }
      }()*/
      return nil
}

func (m *MasterFSM) createLockDomain(d Domain) error {
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
    replicaGroup := lockState.replicaGroup
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
