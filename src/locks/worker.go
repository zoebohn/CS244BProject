package locks

import(
    "raft"
    "fmt"
    "io"
    "encoding/json"
    "bytes"
    "strconv"
    "sync"
    "time"
)

type WorkerFSM struct{
    FsmLock sync.RWMutex
    /* Map of lock to lock state. */
    LockStateMap    map[Lock]lockState
    SequencerMap    map[Lock]Sequencer
    MasterCluster   []raft.ServerAddress
    PeriodStart     time.Time
}

type WorkerSnapshot struct {
    json []byte 
}

type lockState struct{
    /* True if lock is acquired. */
    Held            bool
    /* Address of client holding lock. */
    Client          raft.ServerAddress
    /* True if lock should be moved after released. */
    Recalcitrant    bool
    /* Behaves as though Held by nonexistant client; used for rebalancing */
    Disabled		bool
    /* Count number of accesses in last period. */
    FreqCount       int
    /* Frequency count used to be sent to master. */
    SaveFreqCount         int
}

func CreateWorkers(n int, masterCluster []raft.ServerAddress) ([]raft.FSM) {
    workers := make([]raft.FSM, n)
    for i := range(workers) {
        workers[i] = &WorkerFSM {
            LockStateMap: make(map[Lock]lockState),
            SequencerMap: make(map[Lock]Sequencer),
            MasterCluster: masterCluster,
        }
    }
    return workers
}

func (w *WorkerFSM) Apply(log *raft.Log) (interface{}, func() []byte) { 
    /* Interpret log to find command. Call appropriate function. */
    args := make(map[string]string)
    err := json.Unmarshal(log.Data, &args)
    if err != nil {
        fmt.Println("WORKER: error in apply, ", err) 
    }
    function := args[FunctionKey]
    switch function {
        case ClaimLocksCommand:
            lock_arr := string_to_lock_array(args[LockArrayKey])
            w.claimLocks(lock_arr)
            return nil, nil 
        case DisownLocksCommand:
            lock_arr := string_to_lock_array(args[LockArrayKey])
            w.disownLocks(lock_arr)
            return nil, nil
        case AcquireLockCommand:
            l := Lock(args[LockArgKey])
            clientAddr := raft.ServerAddress(args[ClientAddrKey])
            response, callback := w.tryAcquireLock(l, clientAddr)
            return response, callback
        case ReleaseLockCommand:
            l := Lock(args[LockArgKey])
            clientAddr := raft.ServerAddress(args[ClientAddrKey])
            response, callback := w.releaseLock(l, clientAddr)
            return response, callback
        case ValidateLockCommand:
            l := Lock(args[LockArgKey])
            s, err := strconv.Atoi(args[SequencerArgKey])
            if err != nil {
                fmt.Println("WORKER: error unpacking command")
                return ValidateLockResponse{false, ErrInvalidRequest}, nil
            }
            response := w.validateLock(l, Sequencer(s))
            return response, nil
        case RebalanceCommand:
            lock_arr := string_to_lock_array(args[LockArrayKey])
            response := w.handleRebalanceRequest(lock_arr)
            return response, nil
        case ReleaseForClientCommand:
            c := raft.ServerAddress(args[ClientAddrKey])
            w.releaseForClient(c)
            return nil, nil
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
    w.FsmLock.Lock()
    w.LockStateMap = snapshotRestored.LockStateMap
    w.SequencerMap = snapshotRestored.SequencerMap
    w.MasterCluster = snapshotRestored.MasterCluster
    w.FsmLock.Unlock()
    return nil
}

func (w *WorkerFSM) Snapshot() (raft.FSMSnapshot, error) {
    /* Create snapshot */
    json, json_err := w.convertToJSON()
    if json_err != nil {
        return WorkerSnapshot{json: nil}, json_err
    }
    return WorkerSnapshot{json: json}, nil
}

func (s WorkerSnapshot) Persist(sink raft.SnapshotSink) error {
    /* Write LockStateMap to SnapshotSink */
    _, err := sink.Write(s.json)
    if err != nil {
        sink.Cancel()
        return err
    }

    sink.Close()
    return nil
}

func (s WorkerSnapshot) Release() {
}

func (w *WorkerFSM) convertToJSON() ([]byte, error) {
    w.FsmLock.Lock()
    b, err := json.Marshal(w)
    w.FsmLock.Unlock()
    return b, err
}

func convertFromJSONWorker(byte_arr []byte) (WorkerFSM, error) {
    var w WorkerFSM
    err := json.Unmarshal(byte_arr, &w)
    return w, err
}


func (w *WorkerFSM) tryAcquireLock(l Lock, client raft.ServerAddress) (AcquireLockResponse, func() []byte) {
    w.FsmLock.Lock()
    defer w.FsmLock.Unlock()
    callback := w.updateFreqForOneOp(l)
    fmt.Println("WORKER: trying to acquire lock ", string(l))
     if _, ok := w.LockStateMap[l]; !ok {
         fmt.Println("WORKER: error lock doesn't exist")
         return AcquireLockResponse{-1, ErrLockDoesntExist}, callback
     }
     state := w.LockStateMap[l]
     if state.Held && state.Client == client {
        return AcquireLockResponse{w.SequencerMap[l], ""}, callback
     }
     if state.Held || state.Disabled {
         fmt.Println("WORKER: error lock held or disabled")
         return AcquireLockResponse{-1, ErrLockHeld}, callback
     }
     state.Held = true
     state.Client = client
     w.LockStateMap[l] = state
     w.SequencerMap[l] += 1
     response := AcquireLockResponse{w.SequencerMap[l], ""}
     return response, callback
}

func (w *WorkerFSM) releaseLock(l Lock, client raft.ServerAddress) (ReleaseLockResponse, func() []byte) {
    fmt.Println("WORKER: releasing lock ", string(l))
    w.FsmLock.Lock()
    defer w.FsmLock.Unlock()
    callback := w.updateFreqForOneOp(l)
    if _, ok := w.LockStateMap[l]; !ok {
        return ReleaseLockResponse{ErrLockDoesntExist}, callback
    }
    state := w.LockStateMap[l]
    if !state.Held {
        return ReleaseLockResponse{ErrLockNotHeld}, callback
    }
    if state.Client != client {
        return ReleaseLockResponse{ErrBadClientRelease}, callback
    }
    state.Client = ""
    state.Held = false
    w.LockStateMap[l] = state

    /* Notify master if lock recalcitrant */
    if state.Recalcitrant {
        state.Disabled = true
        w.LockStateMap[l] = state
        // TODO: support returning 2 callbacks!!!
        return ReleaseLockResponse{""}, w.generateRecalcitrantReleaseAlert(l)
    }

    return ReleaseLockResponse{""}, callback
}

func (w *WorkerFSM) validateLock(l Lock, s Sequencer) ValidateLockResponse {
    w.FsmLock.RLock()
    defer w.FsmLock.RUnlock()
    if _, ok := w.LockStateMap[l]; !ok {
        return ValidateLockResponse{false, ErrLockDoesntExist}
    }
    if s == w.SequencerMap[l] {
        return ValidateLockResponse{true, ""}
    } else {
        return ValidateLockResponse{false, ""}
    }
}

func (w *WorkerFSM) claimLocks(lock_arr []Lock) {
    w.FsmLock.Lock()
    defer w.FsmLock.Unlock()
    for _, l := range lock_arr {
        fmt.Println("WORKER: claiming lock ", string(l))
        w.LockStateMap[l] = lockState{Held: false, Client: "", Recalcitrant: false, }
        w.SequencerMap[l] = 0
    }
}

func (w *WorkerFSM) disownLocks(lock_arr []Lock) {
    w.FsmLock.Lock()
    defer w.FsmLock.Unlock()
    for _, l := range lock_arr {
        fmt.Println("WORKER: disowning lock ", string(l))
        delete(w.LockStateMap, l)
    }
}


func (w *WorkerFSM) handleRebalanceRequest(lock_arr []Lock) (RebalanceResponse) {
    w.FsmLock.Lock()
    defer w.FsmLock.Unlock()
    recalcitrantLocks := make(map[Lock]int)
    for _, l := range lock_arr {
        state := w.LockStateMap[l]
        if state.Held {
            state.Recalcitrant = true
            recalcitrantLocks[l] = 1
        } else {
            state.Disabled = true 
        }
        w.LockStateMap[l] = state
    }
    return RebalanceResponse{recalcitrantLocks}
}

func (w *WorkerFSM) generateRecalcitrantReleaseAlert(l Lock) func()[]byte {
    /* Update map */
    /* Send message to master that was released */
    f := func() []byte {
        args := make(map[string]string)
        args[FunctionKey] = ReleasedRecalcitrantCommand
        args[LockArgKey] = string(l)
        command, json_err := json.Marshal(args)
        if json_err != nil {
            //TODO
            fmt.Println("WORKER: JSON ERROR")
        }
        fmt.Println("WORKER: release recalcitrant lock")
        send_err := raft.SendSingletonRequestToCluster(w.MasterCluster, command, &raft.ClientResponse{})
        if send_err != nil {
            fmt.Println("WORKER: error while sending recalcitrant release ")
        }
        return nil
    }
    return f
}

func (w *WorkerFSM) releaseForClient(client raft.ServerAddress) {
    w.FsmLock.Lock()
    defer w.FsmLock.Unlock()
    fmt.Println("WORKER: Releasing locks for client ", client)
    for l := range(w.LockStateMap) {
        state := w.LockStateMap[l]
        if (state.Client == client && state.Held) {
            state.Held = false
            state.Client = ""
            w.LockStateMap[l] = state
        }
    }
}

/* Assumes FSM already locked. */
func (w *WorkerFSM) updateFreqForOneOp(l Lock) func()[]byte {
    lockState := w.LockStateMap[l]
    var result func()[]byte = nil
    /* Check if should enter new period. */
    if (time.Since(w.PeriodStart) >= PERIOD) {
        /* Send stats to master. */
        result = w.sendFrequencyStatsToMaster()
        /* Reset frequency counts to 0. */
        for curr := range w.LockStateMap {
            state := w.LockStateMap[curr]
            state.SaveFreqCount += state.FreqCount
            state.FreqCount = 0
            w.LockStateMap[curr] = state
        }
        /* Reset period start time. */
        w.PeriodStart = time.Now()
    }
    /* Update frequency in current period. */
    lockState.FreqCount++
    w.LockStateMap[l] = lockState
    return result
}

func (w *WorkerFSM) sendFrequencyStatsToMaster() func()[]byte {
    locks := make([]Lock, 0)
    counts := make([]int, 0)
    for l := range w.LockStateMap {
        locks = append(locks, l)
        counts = append(counts, w.LockStateMap[l].SaveFreqCount)
        /* Update saved counts to be 0 for next time sending frequencies. */
        state := w.LockStateMap[l]
        state.SaveFreqCount = 0
        w.LockStateMap[l] = state
    }
    f := func() []byte {
        args := make(map[string]string)
        args[FunctionKey] = FrequencyUpdateCommand
        args[LockArrayKey] = lock_array_to_string(locks)
        args[CountArrayKey] = int_array_to_string(counts)
        command, json_err := json.Marshal(args)
        if json_err != nil {
            //TODO
            fmt.Println("WORKER: JSON ERROR")
        }
        fmt.Println("WORKER: sending frequency update")
        send_err := raft.SendSingletonRequestToCluster(w.MasterCluster, command, &raft.ClientResponse{})
        if send_err != nil {
            fmt.Println("WORKER: error while sending frequency update")
        }
        return nil
    }
    return f
}
