package locks

import(
    "errors"
)

/* Return on lock acquires to let user validate that it still holds lock. */
type Sequencer int

/* Identifies replica group. */
type ReplicaGroupId int

/* Hierarchical name for lock. */
type Lock string

/* Hierarchical lock domain. */
type Domain string

/* TODO: define errors. */
var(
    ErrLockExists = errors.New("lock already exists")
    ErrLockDoesntExist = errors.New("lock doesn't exist")
    ErrNoIntermediateDomain = errors.New("no intermediate domain")
    ErrNoPlacement = errors.New("no valid placement found")
    ErrDomainExists = errors.New("domain already exists")
    ErrEmptyPath = errors.New("cannot use empty path")
    ErrLockHeld = errors.New("lock is currently held")
    ErrLockRecalcitrant = errors.New("lock is recalcitrant")
    ErrLockDisabled = errors.New("lock is currently disabled")
    ErrLockNotHeld = errors.New("lock is not currently held")
    ErrBadClientRelease = errors.New("lock was not acquired by client trying to release it")

)
