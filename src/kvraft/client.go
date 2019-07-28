package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync/atomic"
	"time"
)

var clerkIDGen = int32(0)

// Each client talks to the service through a Clerk with Put/Append/Get methods.
// A Clerk manages RPC interactions with the servers.
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	nServers     int
	leaderServer int

	// 以下两个字段用于协助解决Duplicate detection，使得同一个请求的操作只在状态机上执行一次且仅一次。
	// As soon as you have clients retry operations in the face of errors,
	// you need some kind of **duplicate detection scheme – if a client sends an APPEND to your server,
	// doesn’t hear back, and re-sends it to the next server,
	// your apply() function needs to ensure that the APPEND isn’t executed twice**.
	// To do so, you need some kind of unique identifier for each client request,
	// so that you can recognize if you have seen, and more importantly, applied,
	// a particular operation in the past. **Furthermore, this state needs to be a part of
	// your state machine so that all your Raft servers eliminate the same duplicates**.
	//
	// There are many ways of assigning such identifiers.
	// **One simple and fairly efficient one is to give each client a unique identifier,
	// and then have them tag each request with a monotonically increasing sequence number.
	// If a client re-sends a request, it re-uses the same sequence number.
	// Your server keeps track of the latest sequence number it has seen for each client,
	// and simply ignores any operation that it has already seen**.
	clerkID int32 // clerk id, init by clerkIDGen
	reqID   int64 // request id

	// Clerk中并没有包含一个Mutex，这是因为在测试代码中，Clerk的Get()和PutAppend()同一时刻只有一个会被调用执行，没有竞争条件，无须同步。
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	// 注意servers中的下标为j的labrpc.ClentEnd并不一定与编号为j的labrpc.Server相连，因为
	// config.go中的makeClient()对为该Clerk创建的一组labrpc.ClentEnd数组传入之前使用了random_handles()
	// 打乱了数组中指针元素的顺序，所以查看日志时看到ck.leaderServer与处理RPC的KVServer的编号不一致也不用奇怪。
	ck.servers = servers
	// You'll have to add code here.
	ck.nServers = len(servers)
	ck.leaderServer = 0
	ck.clerkID = atomic.AddInt32(&clerkIDGen, 1)
	ck.reqID = 1

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// keeps trying forever in the face of all other errors.

	// If the Clerk sends an RPC to the wrong kvserver,
	// or if it cannot reach the kvserver, the Clerk should re-try by sending to a different kvserver.

	// If the key/value service commits the operation to its Raft log
	// (and hence applies the operation to the key/value state machine),
	// the leader reports the result to the Clerk by responding to its RPC.

	// If the operation failed to commit (for example, if the leader was replaced),
	// the server reports an error, and the Clerk retries with a different server.

	// It's OK to assume that a client will make only one call into a Clerk at a time.

	args := GetArgs{Key: key, ClerkID: ck.clerkID, ReqID: ck.reqID}
	ck.reqID++
	ok := false
	j := ck.leaderServer
	for {
		reply := GetReply{}
		ch := make(chan bool, 1)
		DPrintf("[Clerk %d Get] args = %+v to KVServer %d", ck.clerkID, args, j)
		// 一直等待。
		go func() {
			ch <- ck.servers[j].Call("KVServer.Get", &args, &reply)
		}()
		select {
		case ok = <-ch:
			if ok {
				DPrintf("[Clerk %d Get] RPC succ, Get %s, reply = %+v", ck.clerkID, key, reply)
				if !reply.WrongLeader {
					ck.leaderServer = j // 更新ck.leaderServer。
					if reply.Err == "" {
						DPrintf("[Clerk %d Get] succ replicate command to replicated state machine", ck.clerkID)
						return reply.Value
					} else if reply.Err == ErrNoKey {
						return "" // returns "" if the key does not exist.
					}
				}
			}
		case <-time.After(RPCTimeout):
			DPrintf("[Clerk %d Get] RPC timeout", ck.clerkID)
		}
		j = (j + 1) % ck.nServers
		time.Sleep(10 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClerkID: ck.clerkID, ReqID: ck.reqID}
	// reply := PutAppendReply{}
	ck.reqID++
	ok := false
	j := ck.leaderServer
	for {
		// 每进行一次RPC，使用一个新的实体供labrpc将RPC结果解码到这个实体中，而不是反复用同一个对象，否则可能reply.WrongLeader永远等于true，
		// 因为false是零值，不会被编码解码，见测试TestGob()。
		reply := PutAppendReply{}
		// 注意，每次发送RPC，要确保ch引用的底层channel实体也是新的。
		// 如果ch这个引用还是原来的引用，即使底层是新的channel，也没有意义。
		// 因为是同一个引用，所以RPC线程能够看到ch的变化，当ch指向新的channel时，前面的旧的RPC线程也看到了。
		// 这样当前面的超时的RPC线程先返回时，写ch，后面的代码将接收到一个默认初始化的reply对象。
		ch := make(chan bool, 1) // 带缓冲的channel，这样timeout后，前面的RPC线程最终返回后写原先的那些ch才不会阻塞，然后线程结束，那些ch被回收。
		DPrintf("[Clerk %d PutAppend] args = %+v to KVServer %d", ck.clerkID, args, j)
		go func() {
			// 引用捕获ch，该线程可以看到ch的变化。
			ch <- ck.servers[j].Call("KVServer.PutAppend", &args, &reply)
		}()
		select {
		case ok = <-ch:
			if ok {
				DPrintf("[Clerk %d PutAppend] RPC succ, %s %s %s, reply = %+v", ck.clerkID, op, key, value, reply)
				if !reply.WrongLeader {
					ck.leaderServer = j
					if reply.Err == "" {
						DPrintf("[Clerk %d PutAppend] succ replicate command to replicated state machine", ck.clerkID)
						return
					}
				}
			}
		case <-time.After(RPCTimeout):
			DPrintf("[Clerk %d PutAppend] RPC timeout", ck.clerkID)
		}
		// 执行到这里，一种情况是网络原因RPC不成功，请求或响应丢了，此时需要换一个server，尝试新的网络连接；
		// 另一种情况是，server原本是leader，但后面变成了stale leader，它Start()的LogEntry被覆盖了，返回错误，
		// 此时也需要尝试新的server。
		j = (j + 1) % ck.nServers
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
