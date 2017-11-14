package client_lib

import (
    "raft"
)

type LockClient struct {
    trans           *raft.NetworkTransport
    masterServers   []raft.ServerAddress
    // TODO: need to go from lock domain to replica group ID
    clients         map[ReplicaGroupId]*raft.Client // Possibly make this a map so know what clients correspond to which domains.
    replicaServers  map[ReplicaGroupId][]raft.ServerAddress
}

/* Return on acquires to let user validate that it still holds lock. */
type Sequencer int

type ReplicaGroupId int

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
    for _, client := range(lc.clients) {
        if err := client.CloseClientSession(); err != nil {
            return err
        }
    }
    return nil
}

func (lc *LockClient) CreateLock(name string) (bool, error) {
    /* Parse name to get domain. */
    /* Contact master to create lock entry (master then contacts replica group). */
    /* Return false if lock already existed. */
    return true, nil
}

func (lc *LockClient) AcquireLock(name string) (Sequencer, error) {
    /* Parse name to get domain. */
    /* If know where lock is stored, open/find connection to contact directly. */
    /* Otherwise, use locate to ask master where stored, then open/find connection. */
    /* Acquire lock and return sequencer. */
    return 1, nil
}

func (lc *LockClient) CreateDomain(name string) (bool, error) {
    /* Parse name to get domain. */
    /* Contact master to create domain (master then contacts replica group). */
    /* Return false if domain already exists. */
    return true, nil
}

/* Helper functions. */

func (lc *LockClient) askMasterToLocate(name string) (ReplicaGroupId, error) {
    /* Ask master for location of lock, return replica group ID. */
    /* Master should return the server addresses of the replica group. */
    /* If server addresses of replica group don't have replica group id yet, put
       in map, ow just return replica group ID. */
    return 1, nil
}

func (lc *LockClient) getClientForId(id ReplicaGroupId) (*raft.Client, error) {
    /* Return existing client session or create new client session for replica group ID. */
    /* Return error if don't have server addresses for replica group ID. */
    return nil, nil
}

// Do we need this??? Depends how we go from lock domains to replica group IDs
func parseLockDomain(name string) ([]string, error) {
    /* Parse string lock domain into different elements.
    /* Error if malformed input. */
    return []string{}, nil
}
