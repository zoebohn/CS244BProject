package locks

import(
    "raft"
    "fmt"
    "io"
    "encoding/json"
    "bytes"
)

type WorkerFSM struct{
    /* Map of lock to lock state. */
    lockStateMap    map[Lock]lockState
    sequencer       Sequencer
}

type WorkerSnapshot struct {
    LockStateMap map[Lock]lockState
    SequencerInt int
}

type lockState struct{
    /* True if lock is acquired. */
    Held            bool
    /* Address of client holding lock. */
    Client          raft.ServerAddress
    /* True if lock should be moved after released. */
    Recalcitrant    bool
}

func CreateWorker(n int) ([]raft.FSM) {
    workers := make([]raft.FSM, n)
    for i := range(workers) {
        workers[i] = &WorkerFSM {
            lockStateMap: make(map[Lock]lockState),
            sequencer: 0,
        }
    }
    return workers
}

func (w *WorkerFSM) Apply(log *raft.Log) (interface{}, func()) { 
    /* Interpret log to find command. Call appropriate function. */
    // use Data and assume it was in json? check type for what
    // function to call? or maybe we add a log command that's a 
    // client command and unpack function type from the data
    args := make(map[string]string)
    err := json.Unmarshal(log.Data, &args)
    if err != nil {
        //TODO
    }
    function := args[FunctionKey]
    l := Lock(args[LockArgKey])
    clientAddr := raft.ServerAddress(args[ClientAddrKey])
    switch function {
        case AddLockCommand:
            w.addLock(l)
            return nil, nil
        case AcquireLockCommand:
            response := w.tryAcquireLock(l, clientAddr)
            return response, nil
        case ReleaseLockCommand:
            response := w.releaseLock(l, clientAddr)
            return response, nil
     }

    return nil, nil
}

func (w *WorkerFSM) Restore(i io.ReadCloser) error {
    var buffer bytes.Buffer
    _, read_err := buffer.ReadFrom(i)
    if read_err != nil {
        return read_err
    }
    snapshotRestored, err := convertFromJSONWorker(buffer.Bytes())
    if err != nil {
        return err
    }
    w.lockStateMap = snapshotRestored.LockStateMap
    w.sequencer = Sequencer(snapshotRestored.SequencerInt)
    return nil
}

func (w *WorkerFSM) Snapshot() (raft.FSMSnapshot, error) {
    /* Create snapshot */
    /* TODO need to lock fsm? */
    s := WorkerSnapshot{LockStateMap: w.lockStateMap, SequencerInt: int(w.sequencer)}
    return s, nil
}

func (s WorkerSnapshot) Persist(sink raft.SnapshotSink) error {
    /* Write lockStateMap to SnapshotSink */
    /* TODO needs to be safe to invoke this with concurrent apply - they actually lock it in there implementation */
    json, json_err := convertToJSON(s)
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
}

func convertToJSON(workerSnapshot WorkerSnapshot) ([]byte, error) {
    b, err := json.Marshal(workerSnapshot)
    return b, err
}

func convertFromJSONWorker(byte_arr []byte) (WorkerSnapshot, error) {
    var workerSnapshot WorkerSnapshot
    err := json.Unmarshal(byte_arr, &workerSnapshot)
    return workerSnapshot, err
}


func (w *WorkerFSM) tryAcquireLock(l Lock, client raft.ServerAddress) (AcquireLockResponse) {
    fmt.Println("WORKER: trying to acquire lock ", string(l))
    /* Check that lock exists, not disabled.
       If not held, acquire and return true.
       Else, return false. */
     if _, ok := w.lockStateMap[l]; !ok {
         return AcquireLockResponse{-1, ErrLockDoesntExist}
     }
     state := w.lockStateMap[l]
     if state.Held {
         return AcquireLockResponse{-1, ErrLockHeld}
     }
     state.Held = true
     state.Client = client
     w.lockStateMap[l] = state
     w.sequencer += 1
     return AcquireLockResponse{w.sequencer, ""}
}

func (w *WorkerFSM) releaseLock(l Lock, client raft.ServerAddress) ReleaseLockResponse {
    fmt.Println("WORKER: releasing lock ", string(l))
    /* Check that lock exists, not disabled.
       If not held by this client, return error.
       If not recalcitrant, release normally. 
       If recalcitrant, mark as disabled and release, notify master. */
    /* TODO: if recalcitrant, tell master and delete (rebalancing protocol). */
    if _, ok := w.lockStateMap[l]; !ok {
        return ReleaseLockResponse{ErrLockDoesntExist}
    }
    state := w.lockStateMap[l]
    if !state.Held {
        return ReleaseLockResponse{ErrLockNotHeld}
    }
    if state.Client != client {
        return ReleaseLockResponse{ErrBadClientRelease}
    }
    state.Client = client
    state.Held = false
    w.lockStateMap[l] = state
    return ReleaseLockResponse{""}
}

func (w *WorkerFSM) addLock(l Lock) {
    fmt.Println("WORKER: adding lock ", string(l))
    w.lockStateMap[l] = lockState{Held: false, Client: "N/A", Recalcitrant: false, }
}

func (w *WorkerFSM) handleRebalanceRequest() {
    //TODO
}

func (w *WorkerFSM) releaseRecalcitrantLock() {
    //TODO
}
