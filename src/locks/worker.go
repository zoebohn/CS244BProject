package locks

import(
    "raft"
    "io"
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

func (w *WorkerFSM) Apply(log *raft.Log) interface{} { 
    /* Interpret log to find command. Call appropriate function. */
    return nil
}

func (w *WorkerFSM) Restore(i io.ReadCloser) error {
    return nil
}

func (w *WorkerFSM) Snapshot() (raft.FSMSnapshot, error) {
    return &workerSnapshot{}, nil
}

func (s *workerSnapshot) Persist(sink raft.SnapshotSink) error {
    return nil
}

func (s *workerSnapshot) Release() {
}

func (w *WorkerFSM) tryAcquireLock(l Lock, client raft.ServerAddress) (bool, error) {
    /* Check that lock exists, not disabled.
       If not held, acquire and return true.
       Else, return false. */
     if _, ok := w.lockStateMap[l]; !ok {
         return false, ErrLockDoesntExist
     }
     state := w.lockStateMap[l]
     if state.held {
         return false, ErrLockHeld
     }
     /* TODO: might not need this check? recalcitrant locks should always be either held or disabled until moved? */
     if state.recalcitrant {
         return false, ErrLockRecalcitrant
     }
     if state.disabled {
         return false, ErrLockDisabled
     }
     state.held = true
     state.client = client
     return true, nil
}

func (w *WorkerFSM) releaseLock(l Lock, client raft.ServerAddress) error {
    /* Check that lock exists, not disabled.
       If not held by this client, return error.
       If not recalcitrant, release normally. 
       If recalcitrant, mark as disabled and release, notify master. */
    return nil
    if _, ok := w.lockStateMap[l]; !ok {
        return ErrLockDoesntExist
    }
    state := w.lockStateMap[l]
    if !state.held {
        return ErrLockNotHeld
    }
    if state.disabled {
        return ErrLockDisabled
    }
    if state.client != client {
        return ErrBadClientRelease
    }
    state.client = client
    state.held = false
    return nil
}
