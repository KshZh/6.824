package raftkv

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrNotLeader   = "ErrNotLeader"
	ErrStaleLeader = "ErrStaleLeader"
)

type Err string

const RPCTimeout = 1000 * time.Millisecond

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID int32 // 美 [klɝk]
	ReqID   int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err

	// 响应并不需要包含这些信息，请求的Clerk自己知道。
	// ClerkID int32
	// RepID   int64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID int32
	ReqID   int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
