package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1

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
	Operation string // Get Put or Append
	Key       string
	Value     string
	ClientID  int32
	ReqID     int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// **查看test_test.go中的测试代码**，键值的类型都是string。
	kvs map[string]string
	// Notify chan for each log index
	notifyCh map[int]chan Op
	// request records
	requests map[int32]int64 // client -> last commited reqID
	// KVServer对象生存期结束时终止它运行时开启的所有线程。
	shutdown chan struct{}
}

// call raft.Start to commit a command as log entry
func (kv *KVServer) proposeCommand(cmd Op) (bool, Err) {
	kv.mu.Lock()
	logIndex, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		kv.mu.Unlock()
		return false, ErrNotLeader
	}
	ch, ok := kv.notifyCh[logIndex]
	if !ok {
		ch = make(chan Op, 1) // buffered channel，避免写入阻塞。
		kv.notifyCh[logIndex] = ch
	}
	kv.mu.Unlock()
	DPrintf("[KVServer %d proposeCommand] cmd = %+v, logIndex = %d", kv.me, cmd, logIndex)

	// check
	if ch == nil {
		panic("FATAL: chan is nil")
	}

	// wait on ch forever, because:
	// If I lose leadership before commit, may be partioned
	// I can't response, so wait until partion healed.
	// Eventually a log will be commited on index, then I'm
	// awaken, but cmd1 is different from cmd, return failed
	// to client.
	// If client retry another leader when I waiting, no matter.
	select {
	case cmd1 := <-ch:
		return cmd1 == cmd, OK
	case <-kv.shutdown:
		return false, ErrStaleLeader
	}
}

// check if repeated request
func (kv *KVServer) isDuplicated(clientID int32, reqID int64) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	maxSeenReqID, ok := kv.requests[clientID]
	if ok {
		return reqID <= maxSeenReqID
	}
	return false
}

// true if update success, imply nonrepeat request can be applied to state machine: eg, data field
func (kv *KVServer) updateIfNotDuplicated(clientID int32, reqID int64) bool {
	// must hold lock outside

	maxSeenReqID, ok := kv.requests[clientID]
	if ok {
		if reqID <= maxSeenReqID {
			return false
		}
	}

	kv.requests[clientID] = reqID
	return true
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[KVServer %d Get] RPC handler, args = %+v", kv.me, *args)
	// check if leader, useless but efficient
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}

	reply.Err = ""
	reply.WrongLeader = false
	reply.ClientID = args.ClientID
	reply.RepID = args.ReqID

	cmd := Op{Operation: "Get", Key: args.Key, Value: "", ClientID: args.ClientID, ReqID: args.ReqID}

	// try commit cmd to raft log
	succ, err := kv.proposeCommand(cmd)
	if succ {
		kv.mu.Lock()
		if v, ok := kv.kvs[args.Key]; ok {
			reply.Value = v
		} else {
			reply.Value = ""
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	} else {
		reply.WrongLeader = true
		reply.Err = err
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	} else if args.Op != "Put" && args.Op != "Append" {
		reply.Err = ErrInvalidOp
		return
	}

	// check if repeated request, useless but efficient
	duplicate := kv.isDuplicated(args.ClientID, args.ReqID)
	if duplicate {
		reply.Err = ErrDuplicateReq
		return
	}

	reply.WrongLeader = false
	reply.Err = ""
	reply.ClientID = args.ClientID
	reply.RepID = args.ReqID

	cmd := Op{Operation: args.Op, Key: args.Key, Value: args.Value, ClientID: args.ClientID, ReqID: args.ReqID}

	succ, err := kv.proposeCommand(cmd)
	if !succ {
		reply.WrongLeader = true
		reply.Err = err
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

func (kv *KVServer) readApplyCh() {
	// 这个线程的生存期与kv server的生存期一样长。
	var op Op
	var applyMsg raft.ApplyMsg
	for {
		select {
		case applyMsg = <-kv.applyCh:
		case <-kv.shutdown:
			DPrintf("[KVServer %d readApplyCh] shutdown", kv.me)
			return
		}
		if !applyMsg.CommandValid {
			// 该KVServer关联的Raft server的快照被leader覆盖了，所以也要更新Raft server关联的KVServer的状态。
			DPrintf("[KVServer %d readApplyCh] update snapshot, kv.kvs = %+v", kv.me, kv.kvs)
			kv.kvs = applyMsg.StateMachineState
			continue
		}
		op, _ = (applyMsg.Command).(Op)

		kv.mu.Lock()
		// Follower & Leader: try apply to state machine, fail if duplicated request.
		// 实现linearizability，每一个cmd执行且仅执行一次。
		update := kv.updateIfNotDuplicated(op.ClientID, op.ReqID)
		if update {
			if op.Operation == "Put" {
				kv.kvs[op.Key] = op.Value
				DPrintf("[KVServer %d readApplyCh] apply for client %d PUT [%s, %s], CommandIndex %d",
					kv.me, op.ClientID, op.Key, op.Value, applyMsg.CommandIndex)
			} else if op.Operation == "Append" {
				kv.kvs[op.Key] += op.Value
				DPrintf("[KVServer %d readApplyCh] apply for client %d APPEND [%s, %s], CommandIndex %d",
					kv.me, op.ClientID, op.Key, kv.kvs[op.Key], applyMsg.CommandIndex)
			} else {
				// Do nothing for Get
				DPrintf("[KVServer %d readApplyCh] apply for client %d Get [%s, %s], CommandIndex %d",
					kv.me, op.ClientID, op.Key, kv.kvs[op.Key], applyMsg.CommandIndex)
			}
		}

		ch, ok := kv.notifyCh[applyMsg.CommandIndex]
		if ok {
			// ch是带缓冲的channel，这里写ch一般不会阻塞。
			ch <- op
		}

		// `kv.rf.persister.RaftStateSize()`，因为persister成员并没有导出raft包，所以这一句是错误的。
		// 给Raft添加一个导出的函数，间接调用rf.persister.RaftStateSize()即可。
		if kv.maxraftstate > 0 && kv.rf.RaftStateSize() >= kv.maxraftstate {
			DPrintf("[KVServer %d readApplyCh] make snapshot, kv.maxraftstate = %d, raft log size %d",
				kv.me, kv.maxraftstate, kv.rf.RaftStateSize())
			// 为了避免fatal error: concurrent map iteration and map write，
			// 这里粗糙地处理为传入kv.kvs的一个副本，注意对map要深拷贝，而不是拷贝指向底层map的引用。
			// 当然，另一个避免的方式是串行调用kv.rf.MakeSnapshotAndPersist()。
			// go kv.rf.MakeSnapshotAndPersist(applyMsg.CommandIndex, simpleMapDeepCopy(kv.kvs), simpleMapDeepCopy1(kv.requests))
			kv.rf.MakeSnapshotAndPersist(applyMsg.CommandIndex, simpleMapDeepCopy(kv.kvs), simpleMapDeepCopy1(kv.requests))
		}
		kv.mu.Unlock()
	}
}

func simpleMapDeepCopy(m map[string]string) map[string]string {
	r := make(map[string]string)
	for k, v := range m {
		r[k] = v
	}
	return r
}

func simpleMapDeepCopy1(m map[int32]int64) map[int32]int64 {
	r := make(map[int32]int64)
	for k, v := range m {
		r[k] = v
	}
	return r
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
	kv.kvs = make(map[string]string)
	kv.notifyCh = make(map[int]chan Op)
	kv.requests = make(map[int32]int64)
	kv.shutdown = make(chan struct{})

	// restart? load persistent state if any.
	kv.rf.LoadStateMachineState(&kv.kvs, &kv.requests)

	// single goroutine which keeps reading applyCh
	go kv.readApplyCh()

	return kv
}
