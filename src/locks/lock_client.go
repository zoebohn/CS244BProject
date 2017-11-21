package locks

import (
    "raft"
    "encoding/json"
    "strconv"
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
    Command         string
    Args            map[string]string
}

func convertClientRPCToJSON(rpc ClientRPC) ([]byte, error) {
    b, err:= json.Marshal(rpc)
    return b, err
}

func (lc *LockClient) CreateLock(l Lock) (bool, error) {
    args := make(map[string]string)
    args[LockArgKey] = string(l)
    rpc := ClientRPC{Command: CreateLockCommand, Args: args}
    json, json_err := convertClientRPCToJSON(rpc)
    if json_err != nil {
        return false, json_err
    }
    /* Parse name to get domain. */
    /* Contact master to create lock entry (master then contacts replica group). */
    /* Return false if lock already existed. */
    return true, nil
}

func (lc *LockClient) AcquireLock(l Lock) (Sequencer, error) {
    args := make(map[string]string)
    args[LockArgKey] = string(l)
    rpc := ClientRPC{Command: AcquireLockCommand, Args: args}
    json, json_err := convertClientRPCToJSON(rpc)
    if json_err != nil {
        return -1, json_err
    }
    /* Parse name to get domain. */
    /* If know where lock is stored, open/find connection to contact directly. */
    /* Otherwise, use locate to ask master where stored, then open/find connection. */
    /* Acquire lock and return sequencer. */
    return 1, nil
}

func (lc *LockClient) ReleaseLock(l Lock) error {
    args := make(map[string]string)
    args[LockArgKey] = string(l)
    rpc := ClientRPC{Command: ReleaseLockCommand, Args: args}
    json, json_err := convertClientRPCToJSON(rpc)
    if json_err != nil {
        return json_err
    }
    /* Parse name to get domain. */
    /* If know where lock is stored, open/find connection to contact directly. */
    /* Otherwise, use locate to ask master where stored, then open/find connection. */
    /* Release lock and return sequencer. */
    return nil
}

func (lc *LockClient) CreateDomain(d Domain) (bool, error) {
    args := make(map[string]string)
    args[DomainArgKey] = string(d)
    rpc := ClientRPC{Command: CreateDomainCommand, Args: args}
    json, json_err := convertClientRPCToJSON(rpc)
    if json_err != nil {
        return false, json_err
    }
    /* Parse name to get domain. */
    /* Contact master to create domain (master then contacts replica group). */
    /* Return false if domain already exists. */
    return true, nil
}

/* Helper functions. */

func (lc *LockClient) askMasterToLocate(l Lock) (ReplicaGroupId, error) {
    args := make(map[string]string)
    args[LockArgKey] = string(l)
    rpc := ClientRPC{Command: LocateLockCommand, Args: args}
    json, json_err := convertClientRPCToJSON(rpc)
    if json_err != nil {
        return -1, json_err
    }
    /* Ask master for location of lock, return replica group ID. */
    /* Master should return the server addresses of the replica group. */
    /* If server addresses of replica group don't have replica group id yet, put
       in map, ow just return replica group ID. */
    return 1, nil
}

func (lc *LockClient) getSessionForId(id ReplicaGroupId) (*raft.Session, error) {
    args := make(map[string]string)
    args[IDArgKey] = strconv.Itoa(int(id))
    rpc := ClientRPC{Command: GetSessionForIDCommand, Args: args}
    json, json_err := convertClientRPCToJSON(rpc)
    if json_err != nil {
        return nil, json_err
    }
    /* Return existing client session or create new client session for replica group ID. */
    /* Return error if don't have server addresses for replica group ID. */
    return nil, nil
}


