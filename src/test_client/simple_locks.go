package main

import (
    "fmt"
    "raft"
    "time"
)

var servers = []raft.ServerAddress {"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}
var LeaderAddr = raft.ServerAddress("127.0.0.1:8000")

func main() {
    trans, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, nil)
    if err != nil {
        fmt.Println("err: ", err)
        return
    }
    c, err := raft.CreateClientSession(trans, servers)
    if err != nil {
        fmt.Println("err: %v", err)
        return
    }
    acquireLock(c)
    releaseLock(c)
    if err := c.CloseClientSession(); err != nil {
        fmt.Println("err: %v", err)
    }
    fmt.Println("done")
/*    acquireLock()
    releaseLock()
    fmt.Println("done")*/
}

func releaseLock(c *raft.Client) {
    fmt.Println("trying to release lock")
    var resp raft.ClientResponse
    err := c.SendRequest([]byte{0}, &resp)
    if err != nil {
        fmt.Println("err: %v", err)
    }
    fmt.Println(resp)
    fmt.Println("released lock")
}

func acquireLock(c *raft.Client) {
    fmt.Println("trying to acquire lock")
    var resp raft.ClientResponse
    err := c.SendRequest([]byte{1}, &resp)
    if err != nil {
        fmt.Println("err: %v", err)
    }
    fmt.Println(resp)
    fmt.Println("acquired lock")
}

func acquireLockNoSession() {
    var resp raft.ClientResponse
    err := raft.MakeClientRequest(LeaderAddr, []byte{1}, &resp)
    if err != nil {
        fmt.Println("err: %v", err)
    }
    fmt.Println(resp)
    fmt.Println("acquired lock")
}

func releaseLockNoSession() {
    var resp raft.ClientResponse
    err := raft.MakeClientRequest(LeaderAddr, []byte{0}, &resp)
    if err != nil {
        fmt.Println("err: %v", err)
    }
    fmt.Println(resp)
    fmt.Println("released lock")
}
