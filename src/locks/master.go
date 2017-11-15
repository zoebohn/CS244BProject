package locks

import(
    "raft"
)

type MasterFSM struct {
    /* Map of locks to replica group where stored. */
    lockMap             map[Lock]ReplicaGroupId
    /* Map of replica group IDs to server addresses of the servers in that replica group. */
    clusterMap          map[ReplicaGroupId][]raft.ServerAddress
    /* Map of lock domains to replica group where should be stored. */
    domainPlacementMap  map[Domain]ReplicaGroupId
    /* Tracks number of locks held by each replica group. */
    numLocksHeld        map[ReplicaGroupId]int
}

/* TODO: what do we need to do here? */
type masterSnapshot struct{}

func (m *MasterFSM) Apply(log *raft.Log) interface{} { 
    /* Interpret log to find command. Call appropriate function. */
    return nil
}

/* TODO: what to do here? */
func (m *MasterFSM) Snapshot() (raft.FSMSnapshot, error) {
    return &masterSnapshot{}, nil
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
      return nil
}

func (m *MasterFSM) createLockDomain(d Domain) error {
    /* Check if already exists (return false).
       Check that intermediate domains exist.
       Get replica group ID where should be put.
       Add to domainPlacementMap. */
    return nil
}

func (m *MasterFSM) findLock(l Lock) (ReplicaGroupId, []raft.ServerAddress, error) {
    /* Check that lock exists.
       Check lockMap to find replica group ID. 
       Return replica groupID and servers using clusterMap. */
    return 1, []raft.ServerAddress{}, nil 
}

func (m *MasterFSM) getParentDomain(d Domain) (Domain, error) {
    /* Strip off last part of domain and get domain before. */
    return "", nil
}

func (m *MasterFSM) recruitCluster() error {
    /* Recruit another cluster. */
    return nil
}
