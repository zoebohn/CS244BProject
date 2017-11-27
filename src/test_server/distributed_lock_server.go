package main

import(
    "locks"
	"raft"
	"os"
    "os/signal"
)

func main() {
    addrs := []raft.ServerAddress{"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}
    locks.MakeCluster(3, locks.CreateMasters(len(addrs)), addrs)
    c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
