package locks

import(
    "raft"
)

/* Client RPCs */
const CreateLockCommand string = "CreateLock"  
const DeleteLockCommand string = "DeleteLock"
const AcquireLockCommand string = "AcquireLock"  
const ReleaseLockCommand string = "RelaseLock" 
const CreateDomainCommand string = "CreateDomainLock" 
const LocateLockCommand string = "LocateLock" 
const ValidateLockCommand string = "ValidateLock"
const ReleaseForClientCommand string = "rel-client"
const FunctionKey string = "function"
const LockArgKey string = "lock"
const DomainArgKey string = "domain"
const SequencerArgKey string = "seq"
const ClientAddrKey string = "client-addr"
const LockArrayKey string = "lock-arr"
const LockArray2Key string = "lock-arr2"
const CountArrayKey string = "count-arr"
const TransactionIDKey string = "trans"
const OldGroupKey string = "old-group"
const NewGroupKey string = "new-group"

/* Master -> Worker RPCs */
const ClaimLocksCommand string = "add-lock"
const TransferCommand string = "transfer"
const DisownLocksCommand string = "disown"

/* Worker -> Master RPCs */
const ReleasedRecalcitrantCommand string = "rel-recal"
const FrequencyUpdateCommand string = "freq-update"

/* Master responses to RPCs */
const TransferLockGroupCommand string = "transfer-group"
const TransferRecalCommand string = "transfer-recal"
const DeleteLockNotAcquiredCommand string = "delete-not-acq"
const DeleteRecalLockCommand string = "delete-recal"

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

type DeleteLockResponse struct {
    ErrMessage string
}

type AcquireLockResponse struct {
    SeqNo Sequencer
    ErrMessage string
}

type ReleaseLockResponse struct {
    ErrMessage string
}

type TransferResponse struct {
    RecalcitrantLocks map[Lock]int
}

type ValidateLockResponse struct {
    Success bool
    ErrMessage string
}

var(
    Success = ""
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
    ErrInvalidRequest = "request not formatted correctly"
)
