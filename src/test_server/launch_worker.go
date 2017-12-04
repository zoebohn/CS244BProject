package main

import(
    "locks"
	"raft"
	"os"
    "os/signal"
    "fmt"
)

func main() {
    workerStrings := os.Args[1:]
    fmt.Println("Launching worker cluster at ", workerStrings)
    if len(workerStrings) != 3 {
        fmt.Println("Need 3 worker addrs")
        return
    }
    workerAddrs := make([]raft.ServerAddress, 0)
    for _, w := range workerStrings {
        workerAddrs = append(workerAddrs, raft.ServerAddress(w))
    }
    addrs := []raft.ServerAddress{"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}
    locks.MakeCluster(3, locks.CreateWorkers(len(workerAddrs), addrs), workerAddrs)
    c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
