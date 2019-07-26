package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync/atomic"
	"time"
)

var clientIDGen = int32(0)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	nServers     int
	leaderServer int
	clientID     int32 // client id, init by clientIdGen
	reqID        int64 // req id

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
	ck.clientID = atomic.AddInt32(&clientIDGen, 1)
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
	// 要明确，看测试用例，知道，Get()只有在rpc成功后才返回，否则一直重试或等待。
	// 复制状态机s就是为了提高fault tolerance，集群要执行一个操作前，先复制到状态机，成功后才能继续。
	args := GetArgs{Key: key, ClientID: ck.clientID, ReqID: ck.reqID}
	ck.reqID++
	ok := false
	j := ck.leaderServer
	for {
		reply := GetReply{}
		ch := make(chan bool, 1)
		DPrintf("[Clerk %d Get] args = %+v to KVServer %d", ck.clientID, args, j)
		// 一直等待。
		go func() {
			ch <- ck.servers[j].Call("KVServer.Get", &args, &reply)
		}()
		select {
		case ok = <-ch:
			if ok {
				DPrintf("[Clerk %d Get] RPC succ, Get %s, reply = %+v", ck.clientID, key, reply)
				if !reply.WrongLeader {
					ck.leaderServer = j
					if reply.Err == "" || reply.Err == ErrDuplicateReq {
						DPrintf("[Clerk %d Get] succ replicate command to replicated state machine", ck.clientID)
						return reply.Value
					} else if reply.Err == ErrNoKey {
						return "" // returns "" if the key does not exist.
					}
				}
			}
		case <-time.After(ElectionTimeout):
			DPrintf("[Clerk %d Get] RPC timeout", ck.clientID)
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
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientID, ReqID: ck.reqID}
	// reply := PutAppendReply{}
	ck.reqID++
	ok := false
	j := ck.leaderServer
	for {
		// 每进行一次RPC，使用一个新的实体供labrpc将RPC结果解码到这个实体中，而不是反复用同一个对象，否则可能reply.WrongLeader永远等于true，
		// 因为false是零值，不会被编码解码，见测试TestGob()。
		reply := PutAppendReply{}
		// 注意，每次发送RPC，ch这个对底层新的channel的引用也是新的。
		// 如果ch这个引用还是原来的引用，即使底层是新的channel，也没有意义。
		// 因为是同一个引用，所以RPC线程能够看到ch的变化，当ch指向新的channel时，前面的旧的RPC线程也看到了。
		// 这样当前面的超时的RPC线程先返回时，写ch，后面的代码将接收到一个默认初始化的reply对象。
		ch := make(chan bool, 1) // 带缓冲的channel，这样timeout后，前面的RPC线程最终返回后写原先的那些ch才不会阻塞，然后线程结束，那些ch被回收。
		DPrintf("[Clerk %d PutAppend] args = %+v to KVServer %d", ck.clientID, args, j)
		go func() {
			ch <- ck.servers[j].Call("KVServer.PutAppend", &args, &reply)
		}()
		select {
		case ok = <-ch:
			if ok {
				DPrintf("[Clerk %d PutAppend] RPC succ, %s %s %s, reply = %+v", ck.clientID, op, key, value, reply)
				if !reply.WrongLeader {
					ck.leaderServer = j
					if reply.Err == "" || reply.Err == ErrDuplicateReq {
						DPrintf("[Clerk %d PutAppend] succ replicate command to replicated state machine", ck.clientID)
						return
					}
				}
			}
		case <-time.After(ElectionTimeout):
			DPrintf("[Clerk %d PutAppend] RPC timeout", ck.clientID)
		}
		// 执行到这里，一种情况是，rpc不成功，server可能出现了分区，所以应该换一个server进行尝试。
		// timeout也需要换一个server进行尝试，否则也会因为分区（stale leader）导致RPC一直失败。
		j = (j + 1) % ck.nServers
		time.Sleep(10 * time.Millisecond)
	}
}

// 考虑一下：
// 2019/06/10 22:45:45 [leader 4/1 sendAndWaitAppendEntriesRPC] replicate LogEntry [{19 1 {Get 0  2 19}}] to peer 1 succ
// 2019/06/10 22:45:45 [leader 1/6 sendAndWaitAppendEntriesRPC] replicate LogEntry [...{19 1 {Get 0  2 19}}] to peer 2 succ
// 2019/06/10 22:45:45 [leader 1/6 calcCommitIndex] follower 2, ai 19, i 19, rf.commit 19
// 2019/06/10 22:45:45 [leader 1/6 sendAndWaitAppendEntriesRPC] replicate LogEntry [...{19 1 {Get 0  2 19}}] to peer 3 succ
// 2019/06/10 22:45:45 [leader 1/6 calcCommitIndex] follower 3, ai 19, i 19, rf.commit 19
// leader不会直接commit旧term的LogEntry。
// 这种情况下，{Get 0  2 19}将永远不会被commit，除非当前term的leader commit了当前term的LogEntry，
// 所以，client要有timeout，以便当遇到这种情况时，再次发送一个新的请求（携带同样的cmd）给leader，
// 让majority agree并让leader最终commit它。
//
// 因此，这样的话我们必须让KVServer实现linearizability，即对同一个cmd，只执行一次，
// 例如对多个有相同ReqID的Append请求，即使它们都commit和apply了，也执行且只执行一次。

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
