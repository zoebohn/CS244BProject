package raft

import (
    //"net"
    "time"
    "fmt"
    "errors"
    //"bufio"

    //"github.com/hashicorp/go-msgpack/codec"
)


type Session struct {
    trans       *NetworkTransport
    currConn    *netConn
    raftServers []ServerAddress
    stopCh      chan bool
    active      bool
}

/* Take address of Raft server and entries in log to be committed.
   Return address of leader if did not reach leader (nil if reached leader),
   error if problem with connection. */
func MakeClientRequest(address ServerAddress, data []byte, resp *ClientResponse) error {
    trans, err := NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, nil)
    // Only establishing connection to itself! It might need to be running on IP addr to connect, or it somehow needs to connect to raft server not itself (hardcode it in again)
    netConn, err := trans.getConn(address)
    if err != nil {
        return err
    }
    // Send RPC
    clientRequest := ClientRequest{
        RPCHeader: RPCHeader{
            ProtocolVersion: ProtocolVersionMax,
        },
        Entries: []*Log{
            &Log{
                Type: LogCommand,
                Data: data,
            },
        },
    }
    if err := sendRPC(netConn, rpcClientRequest, clientRequest); err != nil {
        return err
    }
    fmt.Println("sent RPC")
    // Decode response.
    _, err = decodeResponse(netConn, resp)
    fmt.Println("got response")
    return err
}

// Open client session to cluster. Takes clientID, server addresses for all servers in cluster, and returns success or failure.
// Start go routine to periodically send heartbeat messages and switch to new leader when necessary. 
func CreateClientSession(trans *NetworkTransport, addrs []ServerAddress) (*Session, error) {
    session := &Session{
        trans: trans,
        raftServers: addrs,
        active: true,
        stopCh : make(chan bool, 1),
    }
    if err := session.findActiveServer(); err != nil {
        return nil ,err
    }
    go session.sessionKeepAliveLoop()
    return session, nil
}


// make request to open session. Take data and ptr to response and clientID.
// check active!
func (s *Session) SendRequest(data []byte, resp *ClientResponse) error {
    if !s.active {
        return errors.New("Inactive client session.")
    }
    if resp == nil {
        return errors.New("Response is nil")
    }
    req := ClientRequest {
        RPCHeader: RPCHeader {
            ProtocolVersion: ProtocolVersionMax,
        },
        Entries: []*Log{
            &Log {
                Type: LogCommand,
                Data: data,
            },
        },
        ClientAddr: s.trans.LocalAddr(),
        KeepSession: true,
    }
    return s.sendToActiveLeader(&req, resp)
}


/* Close client session. Kill heartbeat go routine. */
func (s *Session) CloseClientSession() error {
    if !s.active {
        return errors.New("Inactive client session")
    }
    s.stopCh <- true
    fmt.Println("closed client session")
    return nil
}

/* Loop to send and receive heartbeat messages. */
func (s *Session) sessionKeepAliveLoop() {
    for s.active {
        select {
        case <-time.After(time.Second):
        case <- s.stopCh:
            s.active = false
        }
        if !s.active {
            fmt.Println("client session no longer active")
            return
        }
        // Send RPC
        heartbeat := ClientRequest{
          RPCHeader: RPCHeader{
              ProtocolVersion: ProtocolVersionMax,
          },
          Entries: nil,
          ClientAddr: s.trans.LocalAddr(),
          KeepSession: true,
        }
        s.sendToActiveLeader(&heartbeat, &ClientResponse{})
    }
    fmt.Println("client session no longer active")
}

func (s *Session) sendToActiveLeader(request *ClientRequest, response *ClientResponse) error {
    var err error = errors.New("")
    retries := 5
    /* Send heartbeat to active leader. Connect to active leader if connection no longer to active leader. */
    for err != nil {
        if retries <= 0 {
            s.active = false
            return errors.New("Failed to find active leader.")
        }
        if s.currConn == nil {
            s.active = false
            return errors.New("No current connection.")
        }
        err = sendRPC(s.currConn, rpcClientRequest, request)
        /* Try another server if server went down. */
        for err != nil {
            if retries <= 0 {
                s.active = false
                return errors.New("Failed to find active leader.")
            }
            err = s.findActiveServer()
            if err != nil {
                s.active = false
                return errors.New("No active server found.")
            }
            retries--
            err = sendRPC(s.currConn, rpcClientRequest, request)
        }
        /* Decode response if necesary. Try new server to find leader if necessary. */
        // TODO: try new way to find leader now from heartbeats
        fmt.Println("waiting for response")
        _, err = decodeResponse(s.currConn, &response)
        if err != nil {
            s.currConn, err = s.trans.getConn(response.LeaderAddress)
        }
        retries--
    }
    return nil
}

/* Find active raft server and open connection to it. */
func (s *Session) findActiveServer() error {
    var err error
    for _, addr := range(s.raftServers) {
        s.currConn, err = s.trans.getConn(addr)
        if err == nil {
            return nil
        }
    }
    return errors.New("No active raft servers.")
}
