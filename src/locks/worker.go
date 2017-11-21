package locks

import(
    "raft"
    "io"
    "encoding/json"
)

type WorkerFSM struct{
    /* Map of lock to lock state. */
    lockStateMap    map[Lock]lockState
}

type WorkerSnapshot struct {
    lockStateMap map[Lock]lockState
}

type lockState struct{
    /* True if lock is acquired. */
    Held            bool
    /* Address of client holding lock. */
    Client          raft.ServerAddress
    /* True if lock should be moved after released. */
    Recalcitrant    bool
    /* True if disabled (cannot be acquired). */
    Disabled        bool
}

func (w *WorkerFSM) Apply(log *raft.Log) (interface{}, func()) { 
    /* Interpret log to find command. Call appropriate function. */
    return nil, nil
}

func (w *WorkerFSM) Restore(i io.ReadCloser) error {
    /* I think the ReadCloser is from the Snapshot */
    // so read and unmarshal? and then set w?
    byte_arr := make([]byte, 2000) /* TODO make size of snapshot */
    // TODO maybe ned to write size before each snapshot?
    _, read_err := i.Read(byte_arr)
    if read_err != nil {
        return read_err
    }
    // TODO check that bytes_read was long enough
    lockStateMapRestored, err := convertFromJSON(byte_arr)
    if err != nil {
        return err
    }
    w.lockStateMap = lockStateMapRestored
    return nil
}

func (w *WorkerFSM) Snapshot() (raft.FSMSnapshot, error) {
    /* Create snapshot */
    s := WorkerSnapshot{lockStateMap: w.lockStateMap}
    return s, nil
}

func (s WorkerSnapshot) Persist(sink raft.SnapshotSink) error {
    /* Write lockStateMap to SnapshotSink */
    /* TODO needs to be safe to invoke this with concurrent apply */
    json, json_err := convertToJSON(s.lockStateMap)
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

func (s WorkerSnapshot) Release() {
    /* Get rid of file from disk? idk */
}

func convertToJSON(lockStateMap map[Lock]lockState) ([]byte, error) {
    // TODO needs work
    b, err := json.Marshal(lockStateMap)
    return b, err
}

func convertFromJSON(byte_arr []byte) (map[Lock]lockState, error) {
    lockStateMap := map[Lock]lockState{}
    err := json.Unmarshal(byte_arr, &lockStateMap)
    return lockStateMap, err
}


func (w *WorkerFSM) tryAcquireLock(l Lock, client raft.ServerAddress) (bool, error) {
    /* Check that lock exists, not disabled.
       If not held, acquire and return true.
       Else, return false. */
     if _, ok := w.lockStateMap[l]; !ok {
         return false, ErrLockDoesntExist
     }
     state := w.lockStateMap[l]
     if state.Held {
         return false, ErrLockHeld
     }
     /* need to check if recalcitrant? recalcitrant locks should always be either held or disabled until moved? */
     if state.Disabled {
         return false, ErrLockDisabled
     }
     state.Held = true
     state.Client = client
     return true, nil
}

func (w *WorkerFSM) releaseLock(l Lock, client raft.ServerAddress) error {
    /* Check that lock exists, not disabled.
       If not held by this client, return error.
       If not recalcitrant, release normally. 
       If recalcitrant, mark as disabled and release, notify master. */
    /* TODO: if recalcitrant, tell master and delete (rebalancing protocol). */
    if _, ok := w.lockStateMap[l]; !ok {
        return ErrLockDoesntExist
    }
    state := w.lockStateMap[l]
    if !state.Held {
        return ErrLockNotHeld
    }
    if state.Disabled {
        return ErrLockDisabled
    }
    if state.Client != client {
        return ErrBadClientRelease
    }
    state.Client = client
    state.Held = false
    return nil
}