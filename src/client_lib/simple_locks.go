package main

import (
    "fmt"
    "raft"
)

const LeaderAddr = "127.0.0.1:60612"

func main() {
    acquireLock()
    acquireLock()
    go releaseLock()
    go releaseLock()
    acquireLock()
    fmt.Println("done")
}

func acquireLock() {
    var resp raft.ClientResponse
    err := raft.MakeClientRequest(LeaderAddr, []byte{1}, &resp)
    if err != nil {
        fmt.Println("err: %v", err)
    }
    fmt.Println(resp)
    fmt.Println("acquired lock")
}

func releaseLock() {
    var resp raft.ClientResponse
    err := raft.MakeClientRequest(LeaderAddr, []byte{0}, &resp)
    if err != nil {
        fmt.Println("err: %v", err)
    }
    fmt.Println(resp)
    fmt.Println("released lock")
}
