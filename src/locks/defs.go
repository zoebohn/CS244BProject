package locks

import(
    "raft"
)

/* Client RPCs */
const CreateLockCommand string = "CreateLock"  
const AcquireLockCommand string = "AcquireLock"  
const ReleaseLockCommand string = "RelaseLock" 
const CreateDomainCommand string = "CreateDomainLock" 
const LocateLockCommand string = "LocateLock" 
const FunctionKey string = "function"
const LockArgKey string = "lock"
const DomainArgKey string = "domain"
const ClientAddrKey string = "client-addr"
const LockArrayKey string = "lock-arr"
const LockArray2Key string = "lock-arr2"
const TransactionIDKey string = "trans"
const OldGroupKey string = "old-group"
const NewGroupKey string = "new-group"

/* Master -> Worker RPCs */
const ClaimLocksCommand string = "add-lock"
const RebalanceCommand string = "rebalance"
const DisownLocksCommand string = "disown"

/* Worker -> Master RPCs */
const ReleasedRecalcitrantCommand string = "rel-recal"

/* Master responses to RPCs */
const TransferLockGroupCommand string = "transfer-group"
const TransferRecalCommand string = "transfer-recal"


/* Return on lock acquires to let user validate that it still holds lock. */
type Sequencer int

/* Identifies replica group. */
type ReplicaGroupId int

/* Hierarchical name for lock. */
type Lock string

/* Hierarchical lock domain. */
type Domain string


type LocateLockResponse struct {
    ReplicaId ReplicaGroupId
    ServerAddrs []raft.ServerAddress
    ErrMessage string 
}

type CreateDomainResponse struct {
    ErrMessage string
}

type CreateLockResponse struct {
    ErrMessage string
}

type AcquireLockResponse struct {
    SeqNo Sequencer
    ErrMessage string
}

type ReleaseLockResponse struct {
    ErrMessage string
}

type RebalanceResponse struct {
    RecalcitrantLocks map[Lock]int
}

type ClaimLocksResponse struct {
    LocksAdded string
}

/* TODO: define errors. */
var(
    ErrLockExists = "lock already exists"
    ErrLockDoesntExist = "lock doesn't exist"
    ErrNoIntermediateDomain = "no intermediate domain"
    ErrNoPlacement = "no valid placement found"
    ErrDomainExists = "domain already exists"
    ErrEmptyPath = "cannot use empty path"
    ErrLockHeld = "lock is currently held"
    ErrLockRecalcitrant = "lock is recalcitrant"
    ErrLockNotHeld = "lock is not currently held"
    ErrBadClientRelease = "lock was not acquired by client trying to release it"
    ErrNoServersForId = "can't find servers associated with replica id"
    ErrCannotLocateLock = "cannot locate lock"
)
