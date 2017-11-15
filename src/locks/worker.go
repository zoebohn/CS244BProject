package locks

import(
    "raft"
)

type WorkerFSM struct{
    /* Map of lock to lock state. */
    lockStateMap    map[Lock]lockState
}

type lockState struct{
    /* True if lock is acquired. */
    held            bool
    /* Address of client holding lock. */
    client          raft.ServerAddress
    /* True if lock should be moved after released. */
    recalcitrant    bool
    /* True if disabled (cannot be acquired). */
    disabled        bool
}

/* TODO: what do we need to do here? */
type workerSnapshot struct{}

func (w *WorkerFSM) Snapshot() (raft.FSMSnapshot, error) {
    return &workerSnapshot{}, nil
}

func (s *workerSnapshot) Persist(sink raft.SnapshotSink) error {
    return nil
}

func (s *workerSnapshot) Release() {
}

func (w *WorkerFSM) tryAcquireLock(l Lock) (bool, error) {
    /* Check that lock exists, not disabled.
       If not held, acquire and return true.
       Else, return false. */
    return true, nil
}

func (w *WorkerFSM) releaseLock(l Lock) error {
    /* Check that lock exists, not disabled.
       If not held by this client, return error.
       If not recalcitrant, release normally. 
       If recalcitrant, mark as disabled and release, notify master. */
    return nil
}


