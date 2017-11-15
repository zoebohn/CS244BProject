package locks

/* Return on lock acquires to let user validate that it still holds lock. */
type Sequencer int

/* Identifies replica group. */
type ReplicaGroupId int

/* Hierarchical name for lock. */
type Lock string

/* Hierarchical lock domain. */
type Domain string

/* TODO: define errors. */

