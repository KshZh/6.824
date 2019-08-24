package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync/atomic"
	"time"
)

var clerkIDGen = int32(0)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	// Hint: You should implement duplicate client request detection for RPCs to the shard master.
	// The shardmaster tests don't test this, but the shardkv tests will later use your shardmaster
	// on an unreliable network; you may have trouble passing the shardkv tests if your shardmaster
	// doesn't filter out duplicate RPCs.
	clerkID int32 // clerk id, init by clerkIDGen
	reqID   int64 // request id
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clerkID = atomic.AddInt32(&clerkIDGen, 1)
	ck.reqID = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{Num: num, ClerkID: ck.clerkID, ReqID: ck.reqID}
	ck.reqID++
	// Your code here.
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			DPrintf("[Clerk %d Query] ok=%t, reply=%+v", ck.clerkID, ok, reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Servers: servers, ClerkID: ck.clerkID, ReqID: ck.reqID}
	ck.reqID++
	// Your code here.
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			DPrintf("[Clerk %d Join] ok=%t, reply=%+v", ck.clerkID, ok, reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{GIDs: gids, ClerkID: ck.clerkID, ReqID: ck.reqID}
	ck.reqID++
	// Your code here.
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			DPrintf("[Clerk %d Leave] ok=%t, reply=%+v", ck.clerkID, ok, reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{Shard: shard, GID: gid, ClerkID: ck.clerkID, ReqID: ck.reqID}
	ck.reqID++
	// Your code here.
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			DPrintf("[Clerk %d Move] ok=%t, reply=%+v", ck.clerkID, ok, reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
