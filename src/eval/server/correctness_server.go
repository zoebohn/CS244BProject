package main

import(
    "locks"
	"raft"
	"os"
    "os/signal"
)

func main() {
    addrs := []raft.ServerAddress{"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}
   var recruitAddrs [][]raft.ServerAddress = [][]raft.ServerAddress{{"127.0.0.1:6000", "127.0.0.1:6001", "127.0.0.1:6002"}, {"127.0.0.1:6003", "127.0.0.1:6004", "127.0.0.1:6005"}, {"127.0.0.1:6006", "127.0.0.1:6007", "127.0.0.1:6008", "127.0.0.1:6009"}, {"127.0.0.1:6010", "127.0.0.1:6011", "127.0.0.1:6012"}, {"127.0.0.1:6013", "127.0.0.1:6014", "127.0.0.1:6015"}, {"127.0.0.1:6016", "127.0.0.1:6017", "127.0.0.1:6018"}, {"127.0.0.1:6019", "127.0.0.1:6020", "127.0.0.1:6021"}} 
    locks.MakeCluster(3, locks.CreateMasters(len(addrs), addrs, recruitAddrs, 4), addrs)
    c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
