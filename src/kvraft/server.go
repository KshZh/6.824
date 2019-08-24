package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string // Get/Put/Append
	Key       string
	Value     string
	ClerkID   int32
	ReqID     int64
}

// Each of your key/value servers ("kvservers") will have an associated Raft peer.
// Clerks send Put(), Append(), and Get() RPCs to the kvserver whose associated Raft is the leader.
// The kvserver code submits the Put/Append/Get operation to Raft,
// so that the Raft log holds a sequence of Put/Append/Get operations.
// All of the kvservers execute operations from the Raft log in order,
// applying the operations to their key/value databases;
// the intent is for the servers to maintain identical replicas of the key/value database.
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// **查看test_test.go中的测试代码**，键值的类型都是string。
	kvTable        map[string]string
	duplicateTable map[int32]int64 // clerkID -> latest commited reqID

	notify
	cond *sync.Cond

	shutdown chan struct{}
}

type notify struct {
	commandIndex int
	op           Op
}

// 调用rf.Start()尝试将op复制到majority上后commit。
func (kv *KVServer) proposeOp(op Op) bool {
	kv.mu.Lock()
	commandIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}
	kv.mu.Unlock()
	DPrintf("[KVServer %d proposeOp] op = %+v, commandIndex = %d", kv.me, op, commandIndex)

	// Hint: After calling Start(), your kvservers will need to wait for Raft to complete agreement.

	// Hint: Your solution needs to handle the case in which a leader has called Start() for a Clerk's RPC,
	// but loses its leadership before the request is committed to the log.
	// In this case you should arrange for the Clerk to re-send the request to other servers
	// until it finds the new leader. One way to do this is for the server to detect that it has lost leadership,
	// by noticing that a different request has appeared at the index returned by Start(),
	// or that Raft's term has changed. If the ex-leader is partitioned by itself,
	// it won't know about new leaders; but any client in the same partition won't be able to talk to
	// a new leader either, so it's OK in this case for the server and client to wait indefinitely
	// until the partition heals.

	// guide:
	// how do you know when a client operation has completed? In the case of no failures,
	// this is simple – you just wait for the thing you put into the log to come back out (i.e., be passed to apply()).
	// When that happens, you return the result to the client. However, what happens if there are failures?
	// For example, you may have been the leader when the client initially contacted you,
	// but someone else has since been elected, and the client request you put in the log has been discarded.
	// Clearly you need to have the client try again, but how do you know when to tell them about the error?**
	//
	// One simple way to solve this problem is to record where in the Raft log the client’s operation appears
	// when you insert it. Once the operation at that index is sent to apply(),
	// you can tell whether or not the client’s operation succeeded based on whether the operation
	// that came up for that index is in fact the one you put there. If it isn’t,
	// a failure has happened and an error can be returned to the client.
	kv.cond.L.Lock()
	for commandIndex != kv.commandIndex {
		kv.cond.Wait()
	}
	// 如果不相同，则当前KVServer对应的Raft是stale leader，相同位置上的LogEntey被覆盖了，这时让RPC返回错误，让Clerk重发。
	result := kv.op == op
	kv.cond.L.Unlock()
	return result
}

// 循环该请求的操作是否已经执行过。
func (kv *KVServer) execute(clerkID int32, reqID int64) bool {
	// 约定caller持锁调用。
	latestReqID, ok := kv.duplicateTable[clerkID]
	if ok && reqID <= latestReqID {
		return false // duplicate request，该操作已经执行过了，不再执行。
	}
	// fatal error: concurrent map iteration and map write
	// fix bug，因为Raft发送InstallSnapshot时，会访问kvTable和duplicateTable，所以要加锁，避免竞争条件。
	kv.rf.MU.Lock()
	kv.duplicateTable[clerkID] = reqID // 新的request。
	kv.rf.MU.Unlock()
	return true // 非重复请求，执行操作。
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[KVServer %d Get] RPC handler, args = %+v", kv.me, *args)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}

	reply.Err = ""
	reply.WrongLeader = false

	op := Op{Operation: "Get", Key: args.Key, Value: "", ClerkID: args.ClerkID, ReqID: args.ReqID}

	// try commit operation to raft log
	succ := kv.proposeOp(op)

	if succ {
		kv.mu.Lock()
		if v, ok := kv.kvTable[args.Key]; ok {
			reply.Value = v
		} else {
			reply.Value = ""
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	} else {
		reply.WrongLeader = true
		reply.Err = ErrStaleLeader // 或ErrNotLeader。
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}

	reply.WrongLeader = false
	reply.Err = ""

	op := Op{Operation: args.Op, Key: args.Key, Value: args.Value, ClerkID: args.ClerkID, ReqID: args.ReqID}

	succ := kv.proposeOp(op)
	if !succ {
		reply.WrongLeader = true
		reply.Err = ErrStaleLeader // 或ErrNotLeader。
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.shutdown)
	DPrintf("[KVServer %d Kill]", kv.me)
}

// 将已commit的LogEntry按log序执行应用到状态机上。
func (kv *KVServer) apply() {
	// 这个线程的生存期与kv server的生存期一样长。
	var op Op
	var applyMsg raft.ApplyMsg
	for {
		select {
		case applyMsg = <-kv.applyCh:
		case <-kv.shutdown:
			return
		}
		if !applyMsg.CommandValid {
			// 该KVServer关联的Raft server的快照被leader覆盖了，所以也要更新Raft server关联的KVServer的状态。
			kv.kvTable = applyMsg.KVTable
			kv.duplicateTable = applyMsg.DuplicateTable
			DPrintf("[KVServer %d apply] update snapshot, kv.kvTable=%v, kv.duplicateTable=%v", kv.me, kv.kvTable, kv.duplicateTable)
			continue
		}
		op, _ = (applyMsg.Command).(Op)

		kv.mu.Lock()
		if kv.execute(op.ClerkID, op.ReqID) {
			// fatal error: concurrent map iteration and map write
			// fix bug，因为Raft发送InstallSnapshot时，会访问kvTable和duplicateTable，所以要加锁，避免竞争条件。
			kv.rf.MU.Lock()
			if op.Operation == "Put" {
				kv.kvTable[op.Key] = op.Value
				DPrintf("[KVServer %d apply] apply for clerk %d PUT [%s, %s], CommandIndex %d",
					kv.me, op.ClerkID, op.Key, op.Value, applyMsg.CommandIndex)
			} else if op.Operation == "Append" {
				kv.kvTable[op.Key] += op.Value
				DPrintf("[KVServer %d apply] apply for clerk %d APPEND [%s, %s], CommandIndex %d",
					kv.me, op.ClerkID, op.Key, kv.kvTable[op.Key], applyMsg.CommandIndex)
			} else {
				// Do nothing for Get
				DPrintf("[KVServer %d apply] apply for clerk %d Get [%s, %s], CommandIndex %d",
					kv.me, op.ClerkID, op.Key, kv.kvTable[op.Key], applyMsg.CommandIndex)
			}
			kv.rf.MU.Unlock()
		}

		kv.commandIndex = applyMsg.CommandIndex
		kv.op = op
		kv.cond.Broadcast()

		if kv.maxraftstate > 0 && kv.rf.RaftStateSize() >= kv.maxraftstate {
			DPrintf("[KVServer %d apply] make snapshot, kv.maxraftstate = %d, raft log size %d",
				kv.me, kv.maxraftstate, kv.rf.RaftStateSize())
			// 串行持久化，等到持久化当前状态机状态完成后，才可以继续修改状态机。
			kv.rf.MakeSnapshotAndPersist(applyMsg.CommandIndex)
		}
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// kv.kvTable = make(map[string]string) // lab3A
	// kv.duplicateTable = make(map[int32]int64)

	kv.commandIndex = -1
	kv.cond = sync.NewCond(new(sync.Mutex))

	kv.shutdown = make(chan struct{})

	// restart? load persistent state if any.
	// Hint: Your kvserver must be able to detect duplicated operations in the log across checkpoints,
	// so any state you are using to detect them must be included in the snapshots.
	// Remember to capitalize all fields of structures stored in the snapshot.
	kv.kvTable, kv.duplicateTable = kv.rf.LoadSnapshot()

	// single goroutine which keeps reading applyCh
	go kv.apply()

	return kv
}
