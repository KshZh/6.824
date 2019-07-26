package raftkv

import "time"

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrNotLeader    = "ErrNotLeader"
	ErrInvalidOp    = "ErrInvalidOp"
	ErrDuplicateReq = "ErrDuplicateReq"
	ErrStaleLeader  = "ErrStaleLeader"
)

type Err string

const ElectionTimeout = 1000 * time.Millisecond

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int32
	ReqID    int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err

	ClientID int32
	RepID    int64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int32
	ReqID    int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string

	ClientID int32
	RepID    int64
}
