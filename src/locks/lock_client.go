package locks

import (
    "raft"
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

func (lc *LockClient) CreateLock(l Lock) (bool, error) {
    /* Parse name to get domain. */
    /* Contact master to create lock entry (master then contacts replica group). */
    /* Return false if lock already existed. */
    return true, nil
}

func (lc *LockClient) AcquireLock(l Lock) (Sequencer, error) {
    /* Parse name to get domain. */
    /* If know where lock is stored, open/find connection to contact directly. */
    /* Otherwise, use locate to ask master where stored, then open/find connection. */
    /* Acquire lock and return sequencer. */
    return 1, nil
}

func (lc *LockClient) CreateDomain(d Domain) (bool, error) {
    /* Parse name to get domain. */
    /* Contact master to create domain (master then contacts replica group). */
    /* Return false if domain already exists. */
    return true, nil
}

/* Helper functions. */

func (lc *LockClient) askMasterToLocate(l Lock) (ReplicaGroupId, error) {
    /* Ask master for location of lock, return replica group ID. */
    /* Master should return the server addresses of the replica group. */
    /* If server addresses of replica group don't have replica group id yet, put
       in map, ow just return replica group ID. */
    return 1, nil
}

func (lc *LockClient) getSessionForId(id ReplicaGroupId) (*raft.Session, error) {
    /* Return existing client session or create new client session for replica group ID. */
    /* Return error if don't have server addresses for replica group ID. */
    return nil, nil
}

