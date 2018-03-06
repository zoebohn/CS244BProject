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
        fmt.Println("Required 2 arguments, master IP addr, max freq, min freq, ideal freq, max inactive periods, and all worker IP addrs")
        return
    }
    masterIP := args[0]
    maxFreq,err := strconv.ParseFloat(args[1],64)
    if err != nil {
        fmt.Println("bad max freq: ", err)
        return
    }
    minFreq,err := strconv.ParseFloat(args[2],64)
    if err != nil {
        fmt.Println("bad max freq: ", err)
        return
    }
    idealFreq,err := strconv.ParseFloat(args[3],64)
    if err != nil {
        fmt.Println("bad ideal freq: ", err)
        return
    }
    maxInactivePeriods,err := strconv.Atoi(args[4])
    if err != nil {
        fmt.Println("bad max inactive periods: ", err)
        return
    }
    workerIPs := args[5:]
    masterAddrs := eval.GenerateMasterServerList(masterIP)
    recruitList := make([][]raft.ServerAddress, 0)
    for _,workerIP := range workerIPs {
        fmt.Println("worker ip", workerIP)
        workers := eval.GenerateWorkerServerList(workerIP)
        recruitList = append(recruitList, workers)
    }
    locks.MakeCluster(3, locks.CreateMasters(len(masterAddrs), masterAddrs, recruitList, maxFreq, minFreq, idealFreq, maxInactivePeriods, false), masterAddrs)
    c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
