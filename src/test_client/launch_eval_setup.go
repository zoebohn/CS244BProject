package main 

import(
    "locks"
    "raft"
    "fmt"
    "os"
    "os/signal"
    "time"
    "sync"
    "strconv"
)

 var masterServers = []raft.ServerAddress {"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}

var smallLocks = [][]locks.Lock{{locks.Lock("0"), locks.Lock("1"), locks.Lock("2")}, {locks.Lock("3"), locks.Lock("4"), locks.Lock("5")}, {locks.Lock("6"), locks.Lock("7"), locks.Lock("8")}}

func main() {
    args := os.Args[1:]
    if len(args) != 1 {
        fmt.Println("Need 1 argument, test number")
        return
    }
    testNum,err := strconv.Atoi(args[0])
    if err != nil {
        fmt.Println("err with arg: ", err)
        return
    }
    var lockList [][]locks.Lock
    if testNum == 0 {
        lockList = smallLocks
    }
    createLocks(lockList)
}

func createLocks(lockList [][]locks.Lock) {
   trans, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, nil)
    if err != nil {
        fmt.Println("err: ", err)
        return
    }
    lc, lc_err := locks.CreateLockClient(trans, masterServers)
    if lc_err != nil {
        fmt.Println("err: ", lc_err)
    }
    for _,list := range lockList {
        for _, l := range list {
            create_err := lc.CreateLock(l)
            if create_err != nil {
                fmt.Println("err: ", create_err)
            }
        }
    }
    destroy_err := lc.DestroyLockClient()
    if destroy_err != nil {
        fmt.Println("err: ", destroy_err)
    }
}


func smallLockClient(lockList []locks.Lock, printLock *sync.Mutex) {
    trans, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, nil)
    if err != nil {
        fmt.Println("err: ", err)
        return
    }
    lc, lc_err := locks.CreateLockClient(trans, masterServers)
    if lc_err != nil {
        fmt.Println("err: ", lc_err)
    }
    start := time.Now()
    numOps := 0
    c1 := make(chan os.Signal, 1)
    c2 := make(chan os.Signal, 1)
    signal.Notify(c1, os.Interrupt)
    signal.Notify(c2, os.Interrupt)
    go ops_loop(lockList, &numOps, lc, c2)
    <-c1
    end := time.Now()
    printLock.Lock()
    fmt.Println("START: ", start)
    fmt.Println("END: ", end)
    fmt.Println("DURATION (sec): ", (end.Sub(start).Seconds()))
    fmt.Println("NUM OPS: ", numOps)
    fmt.Println("THROUGHPUT (ops/sec): ", float64(numOps) / (end.Sub(start).Seconds()))
    printLock.Unlock()
}

func ops_loop(locks []locks.Lock, numOps *int, lc *locks.LockClient, c chan os.Signal) {
    for true {
        for _,l := range locks {
            select {
            case <-c:
                fmt.Println("done")
                return
            default:
                seq,acq_err := lc.AcquireLock(l)
                if acq_err == nil {
                    *numOps++
                }
                if seq >= 0 {
                    rel_err := lc.ReleaseLock(l)
                    if rel_err == nil {
                        *numOps++
                    }
                }
            }
        }
    }
}
