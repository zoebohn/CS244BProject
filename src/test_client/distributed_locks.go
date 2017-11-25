package main

import (
    "fmt"
    "raft"
    "time"
    "locks"
)

var masterServers = []raft.ServerAddress {"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}

func main() {
    trans, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, nil)
    if err != nil {
        fmt.Println("err: ", err)
        return
    }
    lc, err := locks.CreateLockClient(trans, masterServers)
    if err != nil {
        //TODO
    }
    lock := locks.Lock("test_lock")
    fmt.Println("create lock")
    lc.CreateLock(lock)
    fmt.Println("acquire lock")
    lc.AcquireLock(lock)
    fmt.Println("release lock")
    lc.ReleaseLock(lock)
}
