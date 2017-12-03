package raft

import (
    "net"
    "time"
    "fmt"
    "errors"
    "bufio"

    "github.com/hashicorp/go-msgpack/codec"
)

type Session struct {
    trans               *NetworkTransport
    currConn            *netConn
    raftServers         []ServerAddress
    stopCh              chan bool
    active              bool
    endSessionCommand   []byte
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
    // Decode response.
    _, err = decodeResponse(netConn, resp)
    return err
}

func SendSingletonRequestToCluster(addrs []ServerAddress, data []byte, resp *ClientResponse) error {
    if resp == nil {
        return errors.New("Response is nil")
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
    return sendSingletonRpcToActiveLeader(addrs, &clientRequest, resp)
}


// Open client session to cluster. Takes clientID, server addresses for all servers in cluster, and returns success or failure.
// Start go routine to periodically send heartbeat messages and switch to new leader when necessary. 
func CreateClientSession(trans *NetworkTransport, addrs []ServerAddress, endSessionCommand []byte) (*Session, error) {
    session := &Session{
        trans: trans,
        raftServers: addrs,
        active: true,
        stopCh : make(chan bool, 1),
        endSessionCommand: endSessionCommand,
    }
    var err error
    session.currConn, err = findActiveServerWithTrans(addrs, trans)
    if err != nil {
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
        EndSessionCommand: s.endSessionCommand,
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
          EndSessionCommand: s.endSessionCommand,
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
            s.currConn, err = findActiveServerWithTrans(s.raftServers, s.trans)
            if err != nil {
                s.active = false
                return errors.New("No active server found.")
            }
            retries--
            err = sendRPC(s.currConn, rpcClientRequest, request)
        }
        /* Decode response if necesary. Try new server to find leader if necessary. */
        // TODO: try new way to find leader now from heartbeats
        _, err = decodeResponse(s.currConn, &response)
        if err != nil {
            if response.LeaderAddress != "" {
                s.currConn, _ = s.trans.getConn(response.LeaderAddress)
             } else {
                /* Wait for leader to be elected. */
                time.Sleep(1000*time.Millisecond)
            }
        }
        retries--
    }
    return nil
}

func sendSingletonRpcToActiveLeader(addrs []ServerAddress, request *ClientRequest, response *ClientResponse) error {
    retries := 5 
    conn, err := findActiveServerWithoutTrans(addrs)
    if err != nil {
        return errors.New("No active serve found.")
    }
    err = errors.New("")
    /* Send heartbeat to active leader. Connect to active leader if connection no longer to active leader. */
    for err != nil {
        if conn == nil {
            return errors.New("No current connection.")
        }
        if retries <= 0 {
            conn.conn.Close()
            return errors.New("Failed to find active leader.")
        }
        err = sendRPC(conn, rpcClientRequest, request)
        /* Try another server if server went down. */
        for err != nil {
            fmt.Println("*******error sending: ", err)
            if retries <= 0 {
                conn.conn.Close()
                return errors.New("Failed to find active leader.")
            }
            conn, err = findActiveServerWithoutTrans(addrs)
            if err != nil {
                if conn != nil {
                    conn.conn.Close()
                }
                return errors.New("No active server found.")
            }
            retries--
            err = sendRPC(conn, rpcClientRequest, request)
        }
        /* Decode response if necesary. Try new server to find leader if necessary. */
        // TODO: try new way to find leader now from heartbeats
        _, err = decodeResponse(conn, &response)
        if err != nil {
            if response.LeaderAddress != "" {
                conn, _ = buildNetConn(response.LeaderAddress)
             } else {
                 /* Wait for leader to be elcted. */
                 time.Sleep(1000*time.Millisecond)
            }
        }
        retries--
    }
    conn.conn.Close()
    return nil
}

func findActiveServerWithTrans(addrs []ServerAddress, trans *NetworkTransport) (*netConn, error) {
    for _, addr := range(addrs) {
        conn, err := trans.getConn(addr)
        if err == nil {
            return conn, nil
        }
    }
    return nil, errors.New("No active raft servers.")
}

func findActiveServerWithoutTrans(addrs []ServerAddress) (*netConn, error) {
    for _, addr := range(addrs) {
        conn, err := buildNetConn(addr)
        if err == nil {
            return conn, nil
        }
        conn.conn.Close()
    }
    return nil, errors.New("No active raft servers.")
}

func buildNetConn(target ServerAddress) (*netConn, error) {
    // Dial a new connection
	conn, err := net.Dial("tcp", string(target))
	if err != nil {
		return nil, err
	}

	// Wrap the conn
	netConn := &netConn{
		target: target,
		conn:   conn,
		r:      bufio.NewReader(conn),
		w:      bufio.NewWriter(conn),
	}

	// Setup encoder/decoders
	netConn.dec = codec.NewDecoder(netConn.r, &codec.MsgpackHandle{})
	netConn.enc = codec.NewEncoder(netConn.w, &codec.MsgpackHandle{})

	// Done
	return netConn, nil
}
