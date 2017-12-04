package main 

import(
    "locks"
    "raft"
    "fmt"
    "os"
    "time"
    "strconv"
    "eval"
)

func main() {
    args := os.Args[1:]
    if len(args) != 5 {
        fmt.Println("Need 5 arguments, client IP addr, master IP addr, number of locks per client, number of ttoal clients, and if should use different domains (0 or 1)")
        return
    }
    clientIP := args[0]
    masterIP := args[1]
    numLocksPerClient, err1 := strconv.Atoi(args[2])
    totalClients, err2 := strconv.Atoi(args[3])
    diffDomainsNum, err3 := strconv.Atoi(args[4])
    if err1 != nil || err2 != nil || err3 != nil{
        fmt.Println("Arguments not valid numbers")
        return
    }
    diffDomains := diffDomainsNum != 0
    clientAddr := raft.ServerAddress(clientIP + ":0")
    masterAddrs := eval.GenerateMasterServerList(masterIP)
    lockList := eval.GenerateLockList(numLocksPerClient, totalClients, diffDomains)
    createLocks(lockList, clientAddr, masterAddrs)
}

func createLocks(lockList [][]locks.Lock, clientAddr raft.ServerAddress, masterAddrs []raft.ServerAddress) {
   trans, err := raft.NewTCPTransport(string(clientAddr), nil, 2, time.Second, nil)
    if err != nil {
        fmt.Println("err: ", err)
        return
    }
    lc, lc_err := locks.CreateLockClient(trans, masterAddrs)
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
