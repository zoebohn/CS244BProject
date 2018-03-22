package main

import(
    "locks"
	"raft"
	"os"
    "os/signal"
    //"eval"
    "fmt"
    "strconv"
    "time"
)

func main() {
    args := os.Args[1:]
    if len(args) < 2 {
        fmt.Println("Required 2 arguments, master IP addr, max freq, min freq, ideal freq, max inactive periods, and all worker IP addrs")
        return
    }
    maxFreq,err := strconv.ParseFloat(args[3],64)
    if err != nil {
        fmt.Println("bad max freq: ", err)
        return
    }
    minFreq,err := strconv.ParseFloat(args[4],64)
    if err != nil {
        fmt.Println("bad max freq: ", err)
        return
    }
    idealFreq,err := strconv.ParseFloat(args[5],64)
    if err != nil {
        fmt.Println("bad ideal freq: ", err)
        return
    }
    maxInactivePeriods,err := strconv.Atoi(args[6])
    if err != nil {
        fmt.Println("bad max inactive periods: ", err)
        return
    }
    workerIPs := args[7:]
    masterAddrs := make([]raft.ServerAddress, 0)
    for i := 0; i < 3; i++ {
        masterAddrs = append(masterAddrs, raft.ServerAddress(args[i]))
    }
    recruitList := make([][]raft.ServerAddress, 0)
    for i := 0; i < 3; i++ {
        list := make([]raft.ServerAddress, 0)
        for j := 0; j < 3; j++ {
            list = append(list, raft.ServerAddress(workerIPs[i*3 + j]))
        }
        recruitList = append(recruitList, list)
    }
    transports := make([]*raft.NetworkTransport, len(masterAddrs))
    for i := range masterAddrs {
        trans, err := raft.NewTCPTransport(string(masterAddrs[i]), nil, 2, time.Second, nil)
        if err != nil {
            fmt.Println("err : ", err)
        }
        transports[i] = trans
    }
    locks.MakeCluster(3, locks.CreateMasters(len(masterAddrs), masterAddrs, recruitList, maxFreq, minFreq, idealFreq, maxInactivePeriods, false, transports), masterAddrs, transports)
    c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
