package main

import(
    "locks"
	"os"
    "os/signal"
    "fmt"
    //"eval"
    "raft"
    "time"
)

func main() {
    args := os.Args[1:]
    if len(args) < 4 {
        fmt.Println("Need master IP addr and worker IP addr-port pairs")
        return
    }
    masterAddrs := make([]raft.ServerAddress, 0)
    for i := 0; i < 3; i++ {
        masterAddrs = append(masterAddrs, raft.ServerAddress(args[i]))
    }
    workerAddrs := make([]raft.ServerAddress, 0)
    for i := 0; i < 3; i++ {
        workerAddrs = append(workerAddrs, raft.ServerAddress(args[i+3]))
    }
    fmt.Println("Launching worker cluster at ", workerAddrs)
    transports := make([]*raft.NetworkTransport, len(workerAddrs))
    for i := range workerAddrs {
        trans, err := raft.NewTCPTransport(string(workerAddrs[i]), nil, 2, time.Second, nil)
        if err != nil {
            fmt.Println("err : ", err)
        }
        transports[i] = trans
    }
    locks.MakeCluster(3, locks.CreateWorkers(len(workerAddrs), masterAddrs, workerAddrs, transports), workerAddrs, transports)
    c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
