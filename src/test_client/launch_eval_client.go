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
)

func main() {
    args := os.Args[1:]
    if len(args) != 6 {
        fmt.Println("Need 6 arguments, client IP addr, master IP addr, client number, number of locks per client, number of total clients, and if should use different domains (0 or 1)")
        return
    }
    clientIP := args[0]
    masterIP := args[1]
    clientNum, err1 := strconv.Atoi(args[2])
    numLocksPerClient, err2 := strconv.Atoi(args[3])
    totalClients, err3 := strconv.Atoi(args[4])
    diffDomainsNum, err4 := strconv.Atoi(args[5])
    if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
        fmt.Println("Arguments not valid numbers")
        return
    }
    diffDomains := diffDomainsNum != 0
    clientAddr := raft.ServerAddress(clientIP + ":0")
    masterAddrs := eval.GenerateMasterServerList(masterIP)
    lockList := eval.GenerateLockList(numLocksPerClient, totalClients, diffDomains)
    go runLockClient(lockList[clientNum], clientAddr, masterAddrs)
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt)
    <-c
    time.Sleep(time.Second)
}

func runLockClient(lockList []locks.Lock, clientAddr raft.ServerAddress, masterAddrs []raft.ServerAddress) {
    trans, err := raft.NewTCPTransport(string(clientAddr), nil, 2, time.Second, nil)
    if err != nil {
        fmt.Println("err: ", err)
        return
    }
    lc, lc_err := locks.CreateLockClient(trans, masterAddrs)
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
    fmt.Println("START: ", start)
    fmt.Println("END: ", end)
    fmt.Println("DURATION (sec): ", (end.Sub(start).Seconds()))
    fmt.Println("NUM OPS: ", numOps)
    fmt.Println("THROUGHPUT (ops/sec): ", float64(numOps) / (end.Sub(start).Seconds()))
}

func ops_loop(locks []locks.Lock, numOps *int, lc *locks.LockClient, c chan os.Signal) {
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
