package main

import(
    "locks"
	"raft"
	"os"
    "os/signal"
    "eval"
    "fmt"
    "strconv"
)

func main() {
    args := os.Args[1:]
    if len(args) < 2 {
        fmt.Println("Required 2 arguments, master IP addr and rebalancing factor, and all worker IP addrs")
        return
    }
    masterIP := args[0]
    rebalanceFactor, err := strconv.Atoi(args[1])
    if err != nil {
        fmt.Println("bad rebalance factor: ", err)
        return
    }
    workerIPs := args[2:]
    masterAddrs := eval.GenerateMasterServerList(masterIP)
    recruitList := make([][]raft.ServerAddress, 0)
    for _,workerIP := range workerIPs {
        fmt.Println("worker ip", workerIP)
        workers := eval.GenerateWorkerServerList(workerIP)
        recruitList = append(recruitList, workers)
    }
    locks.MakeCluster(3, locks.CreateMasters(len(masterAddrs), masterAddrs, recruitList, rebalanceFactor, false), masterAddrs)
    c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
