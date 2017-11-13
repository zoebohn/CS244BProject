package raft

import (
    //"net"
    "time"
    "fmt"
    //"bufio"

    //"github.com/hashicorp/go-msgpack/codec"
)

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
// Need to store global state about client states.

// make request to open session. Take data and ptr to response and clientID.

// close session. Needs clientID. Need to kill go routine sending heartbeat messages.
