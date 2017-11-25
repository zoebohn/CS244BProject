package locks

import (
    "raft"
    "fmt"
    "errors"
    "encoding/json"
)

type LockClient struct {
    trans           *raft.NetworkTransport
    masterServers   []raft.ServerAddress
    // TODO: need to go from lock domain to replica group ID
    locks           map[Lock]ReplicaGroupId
    sessions        map[ReplicaGroupId]*raft.Session // Possibly make this a map so know what clients correspond to which domains.
    replicaServers  map[ReplicaGroupId][]raft.ServerAddress
}

// TODO: use some sort of client ID (trans.LocalAddr()?) to identify lock client in requests.

/* Create lock client. */
func CreateLockClient(trans *raft.NetworkTransport, masterServers []raft.ServerAddress) (*LockClient, error) {
    lc := &LockClient {
        trans:          trans,
        masterServers:  masterServers,
        locks:          make(map[Lock]ReplicaGroupId),
        sessions:       make(map[ReplicaGroupId]*raft.Session),
        replicaServers: make(map[ReplicaGroupId][]raft.ServerAddress),
    }
    return lc, nil
}

func (lc *LockClient) DestroyLockClient() error {
    /* Release any acquired locks. */
    /* Close client sessions. */
    for _, s := range(lc.sessions) {
        if err := s.CloseClientSession(); err != nil {
            return err
        }
    }
    return nil
}

type ClientRPC struct {
    Command         raft.LogType
    Args            map[string]string
}

/* Worker Requests */

func (lc *LockClient) AcquireLock(l Lock) (Sequencer, error) {
    args := make(map[string]string)
    args[FunctionKey] = AcquireLockCommand
    args[LockArgKey] = string(l)
    args[ClientAddrKey] = string(lc.trans.LocalAddr())
    data, err := json.Marshal(args)
    if err != nil {
        return -1, err
    }
    replicaID, ok := lc.locks[l]
    if !ok {
        fmt.Println("must locate lock")
        new_id, lookup_err := lc.askMasterToLocate(l)
        replicaID = new_id
        if lookup_err != nil {
            fmt.Println("error with lookup")
        } else {
            fmt.Println("AT ID")
            fmt.Println(replicaID)
            lc.locks[l] = replicaID
        }
    }
    session, session_err := lc.getSessionForId(replicaID)
    if session_err != nil {
        return -1, session_err
    }
    resp := raft.ClientResponse{}
    send_err := session.SendRequest(data, &resp)
    if send_err != nil {
        return -1, send_err    
    }
    /* Parse name to get domain. */
    /* If know where lock is stored, open/find connection to contact directly. */
    /* Otherwise, use locate to ask master where stored, then open/find connection. */
    /* Acquire lock and return sequencer. */
    var response AcquireLockResponse
    unmarshal_err := json.Unmarshal(resp.ResponseData, &response)
    if unmarshal_err != nil {
        fmt.Println(resp.ResponseData)
        fmt.Println(response)
        fmt.Println("error unmarshalling acquire")
        //TODO
        fmt.Println(unmarshal_err)
    }
    if response.ErrMessage != "" {
        return response.SeqNo, errors.New(response.ErrMessage)
    }
    return response.SeqNo, nil
}

func (lc *LockClient) ReleaseLock(l Lock) error {
    args := make(map[string]string)
    args[FunctionKey] = ReleaseLockCommand
    args[LockArgKey] = string(l)
    args[ClientAddrKey] = string(lc.trans.LocalAddr())
    data, err := json.Marshal(args)
    if err != nil {
        return err
    }
    replicaID, ok := lc.locks[l]
    if !ok {
        fmt.Println("must locate lock")
        new_id, lookup_err := lc.askMasterToLocate(l)
        replicaID = new_id
        if lookup_err != nil {
            fmt.Println("error with lookup")
        } else {
            lc.locks[l] = replicaID
        }
    }
    session, session_err := lc.getSessionForId(replicaID)
    if session_err != nil {
        return session_err
    }
    resp := raft.ClientResponse{}
    send_err := session.SendRequest(data, &resp)
    if send_err != nil {
        return send_err
    }
    /* Parse name to get domain. */
    /* If know where lock is stored, open/find connection to contact directly. */
    /* Otherwise, use locate to ask master where stored, then open/find connection. */
    /* Release lock and return sequencer. */
    var response ReleaseLockResponse
    unmarshal_err := json.Unmarshal(resp.ResponseData, &response)
    if unmarshal_err != nil {
        fmt.Println("error unmarshalling release")
        //TODO
        fmt.Println(unmarshal_err)
    }
    if response.ErrMessage != "" {
        return errors.New(response.ErrMessage)
    }
    return nil
}

/* Master Requests */

func (lc *LockClient) CreateLock(l Lock) (error) {
    args := make(map[string]string)
    args[FunctionKey] = CreateLockCommand
    args[LockArgKey] = string(l)
    data, err := json.Marshal(args)
    if err != nil {
        return err
    }
    resp := raft.ClientResponse{}
    send_err := raft.SendSingletonRequestToCluster(lc.masterServers, data, &resp)
    if send_err != nil {
        return send_err
    }
    /* Contact master to create lock entry (master then contacts replica group). */
    /* Return false if lock already existed. */
    var response CreateLockResponse
    unmarshal_err := json.Unmarshal(resp.ResponseData, &response)
    if unmarshal_err != nil {
        fmt.Println("error unmarshalling create")
        //TODO
        fmt.Println(unmarshal_err)
    }
    if response.ErrMessage != "" {
        return errors.New(response.ErrMessage)
    }
    return nil
}

func (lc *LockClient) CreateDomain(d Domain) (error) {
    args := make(map[string]string)
    args[FunctionKey] = CreateDomainCommand
    args[DomainArgKey] = string(d)
    data, err := json.Marshal(args)
    if err != nil {
        return err
    }
    resp := raft.ClientResponse{}
    send_err := raft.SendSingletonRequestToCluster(lc.masterServers, data, &resp)
    if send_err != nil {
        return send_err
    }
    /* Contact master to create domain (master then contacts replica group). */
    /* Return false if domain already exists. */
    var response CreateDomainResponse
    unmarshal_err := json.Unmarshal(resp.ResponseData, &response)
    if unmarshal_err != nil {
        fmt.Println("error unmarshalling")
        //TODO
        fmt.Println(unmarshal_err)
    }
    if response.ErrMessage != "" {
        return errors.New(response.ErrMessage)
    }
    return nil
}

/* Helper functions. */

func (lc *LockClient) askMasterToLocate(l Lock) (ReplicaGroupId, error) {
    args := make(map[string]string)
    args[FunctionKey] = LocateLockCommand
    args[LockArgKey] = string(l)
    data, err := json.Marshal(args)
    if err != nil {
        return -1, err
    }
    resp := raft.ClientResponse{}
    send_err := raft.SendSingletonRequestToCluster(lc.masterServers, data, &resp)
    if send_err != nil {
        return -1, send_err
    }
    /* Ask master for location of lock, return replica group ID. */
    /* Master should return the server addresses of the replica group. */
    /* If server addresses of replica group don't have replica group id yet, put
       in map, ow just return replica group ID. */
    var located LocateLockResponse
    unmarshal_err := json.Unmarshal(resp.ResponseData, &located)
    if unmarshal_err != nil {
        fmt.Println("error unmarshalling")
        //TODO
        fmt.Println(unmarshal_err)
    }
    //TODO CHECK FOR ERR FIRST
    if located.ErrMessage != "" {
        return located.ReplicaId, errors.New(located.ErrMessage)
    }
    lc.locks[l] = located.ReplicaId
    lc.replicaServers[located.ReplicaId] = located.ServerAddrs
    return located.ReplicaId, nil
}

func (lc *LockClient) getSessionForId(id ReplicaGroupId) (*raft.Session, error) {
    /* Return existing client session or create new client session for replica group ID. */
    existing := lc.sessions[id]
    if existing != nil {
        return existing, nil
    }
    server_addrs := lc.replicaServers[id] // TODO @EMMA will we always have this?
    new_session, err := raft.CreateClientSession(lc.trans, server_addrs)
    /* Return error if don't have server addresses for replica group ID. */
    return new_session, err
}


