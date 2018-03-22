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
    if len(args) < 5 {
        fmt.Println("Need 5 arguments, master IP addrs, client IP addr, number of locks per client, number of ttoal clients, and if should use different domains (0 or 1)")
        return
    }
    clientIP := args[3]
    masterAddrs := make([]raft.ServerAddress, 0)
    for i := 0; i < 3; i++ {
         masterAddrs = append(masterAddrs, raft.ServerAddress(args[i]))
    }
    numLocksPerClient, err1 := strconv.Atoi(args[4])
    totalClients, err2 := strconv.Atoi(args[5])
    diffDomainsNum, err3 := strconv.Atoi(args[6])
    if err1 != nil || err2 != nil || err3 != nil{
        fmt.Println("Arguments not valid numbers")
        return
    }
    diffDomains := diffDomainsNum != 0
    clientAddr := raft.ServerAddress(clientIP + ":0")
    lockList := eval.GenerateLockList(numLocksPerClient, totalClients, diffDomains)
    domainList := eval.GenerateDomainList(totalClients, diffDomains)
    createLocksAndDomains(lockList, domainList, clientAddr, masterAddrs)
}

func createLocksAndDomains(lockList [][]locks.Lock, domainList []locks.Domain, clientAddr raft.ServerAddress, masterAddrs []raft.ServerAddress) {
   trans, err := raft.NewTCPTransport(string(clientAddr), nil, 2, time.Second, nil)
    if err != nil {
        fmt.Println("err: ", err)
        return
    }
    lc, lc_err := locks.CreateLockClient(trans, masterAddrs)
    if lc_err != nil {
        fmt.Println("err: ", lc_err)
    }
    for _, d := range domainList {
        create_err := lc.CreateDomain(d)
        if create_err != nil {
            fmt.Println("err: ", create_err)
        }
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
