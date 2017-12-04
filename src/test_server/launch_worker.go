package main

import(
    "locks"
	"os"
    "os/signal"
    "fmt"
    "eval"
)

func main() {
    args := os.Args[1:]
    if len(args) != 2 {
        fmt.Println("Need master IP addr and worker IP addr")
        return
    }
    masterIP := args[0]
    workerIP := args[1]
    workerAddrs := eval.GenerateWorkerServerList(workerIP)
    fmt.Println("Launching worker cluster at ", workerAddrs)
    masterAddrs := eval.GenerateMasterServerList(masterIP)
    locks.MakeCluster(3, locks.CreateWorkers(len(workerAddrs), masterAddrs), workerAddrs)
    c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
