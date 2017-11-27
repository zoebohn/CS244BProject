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
        fmt.Println("error with creating lock client")
        fmt.Println(err)
    } else {
        fmt.Println("successfully created lock client")
    }
    lock := locks.Lock("test_lock")
    fmt.Println("create lock")
    create_err := lc.CreateLock(lock)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created lock")
    }
    fmt.Println("acquire lock")
    id, acquire_err := lc.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println("error with acquiring")
        fmt.Println(acquire_err)
    } else {
        fmt.Println("successfully acquired lock")
    }
    fmt.Println("release lock")
    release_err := lc.ReleaseLock(lock)
    if release_err != nil {
        fmt.Println("error with releasing")
        fmt.Println(release_err)
    } else {
        fmt.Println("successfully released lock")
    }
}
