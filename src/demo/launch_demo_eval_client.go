package main 

import(
    "locks"
    "raft"
    "fmt"
    "os"
    "os/signal"
    "time"
    "strconv"
    "eval"
    "sync"
)

var totalOps uint64 = 0
var totalOpsLock *sync.Mutex = &sync.Mutex{}

func main() {
    args := os.Args[1:]
    if len(args) < 7 {
        fmt.Println("Need 7 arguments, client IP addr, master IP addr, client number, number of locks per client, number of total clients, and if should use different domains (0 or 1), and how many threads to create")
        return
    }
    clientIP := args[3]
    masterAddrs := make([]raft.ServerAddress, 0)
    for i := 0; i < 3; i++ {
        masterAddrs = append(masterAddrs, raft.ServerAddress(args[i]))
    }
    clientNum, err1 := strconv.Atoi(args[4])
    numLocksPerClient, err2 := strconv.Atoi(args[5])
    totalClients, err3 := strconv.Atoi(args[6])
    diffDomainsNum, err4 := strconv.Atoi(args[7])
    numThreads, err5 := strconv.Atoi(args[8])
    if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
        fmt.Println("Arguments not valid numbers")
        return
    }
    diffDomains := diffDomainsNum != 0
    clientAddr := raft.ServerAddress(clientIP + ":0")
    lockList := eval.GenerateLockList(numLocksPerClient, totalClients * numThreads, diffDomains)
    for i := 0; i < numThreads; i++ {
        go runLockClient(lockList[(clientNum * numThreads) + i], clientAddr, masterAddrs, "client_eval_" + strconv.Itoa(clientNum))
    }
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt)
    go waitOneMinute(c)
    <-c
    time.Sleep(time.Second)
    f, err := os.OpenFile("client_eval_" + strconv.Itoa(clientNum), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
    if err != nil {
        fmt.Println("error creating/opening file: ", err)
        return
    }
    defer f.Close()
    totalOpsLock.Lock()
    fmt.Fprintf(f, "# locks per client = %d, total # clients = %d, # threads per client = %d, different domains = %d, numOps = %d\n", numLocksPerClient, totalClients, numThreads, diffDomainsNum, totalOps)
    fmt.Println("NUM OPS: ", totalOps)
    totalOpsLock.Unlock()

}

func runLockClient(lockList []locks.Lock, clientAddr raft.ServerAddress, masterAddrs []raft.ServerAddress, filename string) {
    trans, err := raft.NewTCPTransport(string(clientAddr), nil, 2, time.Second, nil)
    if err != nil {
        fmt.Println("err: ", err)
        return
    }
    lc, lc_err := locks.CreateLockClient(trans, masterAddrs)
    if lc_err != nil {
        fmt.Println("err: ", lc_err)
    }
    var numOps uint64
    numOps = 0
    c1 := make(chan os.Signal, 1)
    c2 := make(chan os.Signal, 1)
    signal.Notify(c1, os.Interrupt)
    signal.Notify(c2, os.Interrupt)
    go waitOneMinute(c1)
    go waitOneMinute(c2)
    go ops_loop(lockList, &numOps, lc, c2)
    <-c1
    totalOpsLock.Lock()
    totalOps += numOps
    totalOpsLock.Unlock()
}

func waitOneMinute(c chan os.Signal) {
    timer := time.NewTimer(2*time.Minute)
    <-timer.C
    c <- os.Interrupt
}

func ops_loop(locks []locks.Lock, numOps *uint64, lc *locks.LockClient, c chan os.Signal) {
    for true {
        for _,l := range locks {
            select {
            case <-c:
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
