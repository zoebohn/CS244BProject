package locks

import(
    "raft"
    "fmt"
    "io"
    "encoding/json"
    "bytes"
    "strings"
)

type WorkerFSM struct{
    /* Map of lock to lock state. */
    lockStateMap    map[Lock]lockState
    sequencer       Sequencer
    masterCluster   []raft.ServerAddress
}

type WorkerSnapshot struct {
    LockStateMap map[Lock]lockState
    SequencerInt int
    MasterCluster []raft.ServerAddress
}

type lockState struct{
    /* True if lock is acquired. */
    Held            bool
    /* Address of client holding lock. */
    Client          raft.ServerAddress
    /* True if lock should be moved after released. */
    Recalcitrant    bool
}

func CreateWorkers(n int, masterCluster []raft.ServerAddress) ([]raft.FSM) {
    workers := make([]raft.FSM, n)
    for i := range(workers) {
        workers[i] = &WorkerFSM {
            lockStateMap: make(map[Lock]lockState),
            sequencer: 0,
            masterCluster: masterCluster,
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
    switch function {
        case AddLockCommand:
            l := Lock(args[LockArgKey])
            w.addLock(l)
            return nil, nil
        case AcquireLockCommand:
            l := Lock(args[LockArgKey])
            clientAddr := raft.ServerAddress(args[ClientAddrKey])
            response := w.tryAcquireLock(l, clientAddr)
            return response, nil
        case ReleaseLockCommand:
            l := Lock(args[LockArgKey])
            clientAddr := raft.ServerAddress(args[ClientAddrKey])
            response, callback := w.releaseLock(l, clientAddr)
            return response, callback
        case RebalanceCommand:
            locks_to_move_string := strings.Split(args[LocksToMoveKey], ";")
            var locks_to_move []Lock
            for _, l := range locks_to_move_string {
                locks_to_move = append(locks_to_move, Lock(l))
            }
            response := w.handleRebalanceRequest(locks_to_move)
            return response, nil //TODO: idk about this
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
    w.masterCluster = snapshotRestored.MasterCluster
    return nil
}

func (w *WorkerFSM) Snapshot() (raft.FSMSnapshot, error) {
    /* Create snapshot */
    /* TODO need to lock fsm? */
    s := WorkerSnapshot{LockStateMap: w.lockStateMap, SequencerInt: int(w.sequencer), MasterCluster: w.masterCluster}
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

func (w *WorkerFSM) releaseLock(l Lock, client raft.ServerAddress) (ReleaseLockResponse, func()) {
    fmt.Println("WORKER: releasing lock ", string(l))
    /* Check that lock exists, not disabled.
       If not held by this client, return error.
       If not recalcitrant, release normally. 
       If recalcitrant, mark as disabled and release, notify master. */
    /* TODO: if recalcitrant, tell master and delete (rebalancing protocol). */
    if _, ok := w.lockStateMap[l]; !ok {
        return ReleaseLockResponse{ErrLockDoesntExist}, nil
    }
    state := w.lockStateMap[l]
    if !state.Held {
        return ReleaseLockResponse{ErrLockNotHeld}, nil
    }
    if state.Client != client {
        return ReleaseLockResponse{ErrBadClientRelease}, nil
    }
    state.Client = client
    state.Held = false
    w.lockStateMap[l] = state

    if state.Recalcitrant {
        return ReleaseLockResponse{""}, w.releaseRecalcitrantLock(l)
    }

    return ReleaseLockResponse{""}, nil
}

func (w *WorkerFSM) addLock(l Lock) {
    fmt.Println("WORKER: adding lock ", string(l))
    w.lockStateMap[l] = lockState{Held: false, Client: "N/A", Recalcitrant: false, }
}

func (w *WorkerFSM) handleRebalanceRequest(locks_to_move []Lock) (RebalanceResponse) {
    recalcitrantLocks := make(map[Lock]int)
    for _, l := range locks_to_move {
        state := w.lockStateMap[l]
        if state.Held {
            state.Recalcitrant = true
            w.lockStateMap[l] = state
            recalcitrantLocks[l] = 1
        } else {
            delete(w.lockStateMap, l)
        }
    }
    return RebalanceResponse{recalcitrantLocks}
}

func (w *WorkerFSM) releaseRecalcitrantLock(l Lock) func() {
    /* Update map */
    delete(w.lockStateMap, l) //TODO does this need to happen after we do something on the master? idk; maybe we wait for reply (in fsm) before we release and for now just mark it as held by nonexistant client (or use esomething else) 
    /* Send message to master that was released */
    args := make(map[string]string)
    args[FunctionKey] = ReleasedRecalcitrantCommand
    args[LockArgKey] = string(l)
    command, json_err := json.Marshal(args)
    if json_err != nil {
        //TODO
        fmt.Println("WORKER: JSON ERROR")
    }
    f := func() {
        send_err := raft.SendSingletonRequestToCluster(w.masterCluster, command, &raft.ClientResponse{})
        if send_err != nil {
            fmt.Println("WORKER: error while sending recalcitrant release ")
        }
    }
    /* I don't think it needs to block...*/
    return f
}
