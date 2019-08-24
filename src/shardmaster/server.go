package shardmaster

import (
	"bytes"
	"fmt"
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

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs        []Config        // indexed by config num
	duplicateTable map[int32]int64 // clerkID -> latest commited reqID

	notify
	cond    *sync.Cond
	queryCh chan Config

	shutdown chan struct{}
}

type notify struct {
	commandIndex int
	op           Op
}

type Op struct {
	// Your data here.
	Operation string // Join/Leave/Move/Query
	Args      []byte
}

// 调用rf.Start()尝试将op复制到majority上后commit。
func (sm *ShardMaster) proposeOp(op Op) bool {
	sm.mu.Lock()
	commandIndex, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return false
	}
	sm.mu.Unlock()
	DPrintf("[ShardMaster %d proposeOp] op = %+v, commandIndex = %d", sm.me, op, commandIndex)

	sm.cond.L.Lock()
	for commandIndex != sm.commandIndex {
		sm.cond.Wait()
	}
	// 如果不相同，则当前KVServer对应的Raft是stale leader，相同位置上的LogEntey被覆盖了，这时让RPC返回错误，让Clerk重发。
	result := false
	if sm.op.Operation == op.Operation && len(sm.op.Args) == len(op.Args) {
		// slice没有定义比较运算：https://stackoverflow.com/questions/15893250/uncomparable-type-error-when-string-field-used-go-lang
		i := 0
		for ; i < len(op.Args); i++ {
			if sm.op.Args[i] != op.Args[i] {
				break
			}
		}
		if i == len(op.Args) {
			result = true
		}

	}
	// 	switch op.Operation {
	// 	case "JoinArgs":
	// 		if sm.op
	// 		var args JoinArgs
	// 		d := labgob.NewDecoder(bytes.NewBuffer(op.Args))
	// 		if d.Decode(&args) != nil {
	// 			panic("decode failed")
	// 		}
	// 		var args1 JoinArgs
	// 		d2 := labgob.NewDecoder(bytes.NewBuffer(sm.op.Args))
	// 		if d.Decode(&args1) != nil {
	// 			panic("decode failed")
	// 		}
	// 		if args.ClerkID != args1.ClerkID || args.ReqID != args1.ReqID {
	// 			break
	// 		}
	// 		// slice没有定义比较运算：https://stackoverflow.com/questions/15893250/uncomparable-type-error-when-string-field-used-go-lang
	// 		for k, v := range args.Servers {
	// 			if v1, ok := args1.Servers[k]; ok {
	// 				if len(v) == len(v1) {
	// 					i := 0
	// 					for ; i < len(v); i++ {
	// 						if v[i] != v1[i] {
	// 							break
	// 						}
	// 					}
	// 					if i == len(v) {
	// 						result = true
	// 					}
	// 				}
	// 			}
	// 		}
	// 	case "LeaveArgs":
	// 		switch args1 := sm.op.Args.(type) {
	// 		case LeaveArgs:
	// 			if args.ClerkID != args1.ClerkID || args.ReqID != args1.ReqID {
	// 				break
	// 			}
	// 			if len(args.GIDs) == len(args1.GIDs) {
	// 				i := 0
	// 				for ; i < len(args.GIDs); i++ {
	// 					if args.GIDs[i] != args1.GIDs[i] {
	// 						break
	// 					}
	// 				}
	// 				if i == len(args.GIDs) {
	// 					result = true
	// 				}
	// 			}
	// 		}
	// 	case "MoveArgs":
	// 		switch args1 := sm.op.Args.(type) {
	// 		case MoveArgs:
	// 			if args.ClerkID != args1.ClerkID || args.ReqID != args1.ReqID {
	// 				break
	// 			}
	// 			if args.GID == args1.GID && args.Shard == args1.Shard {
	// 				result = true
	// 			}
	// 		}
	// 	case "QueryArgs":
	// 		switch args1 := sm.op.Args.(type) {
	// 		case QueryArgs:
	// 			if args.ClerkID != args1.ClerkID || args.ReqID != args1.ReqID {
	// 				break
	// 			}
	// 			if args.Num == args1.Num {
	// 				result = true
	// 			}
	// 		}
	// 	}
	// }
	sm.cond.L.Unlock()
	return result
}

func (sm *ShardMaster) deepCopyConfig(num int) Config {
	// 要求caller加锁。
	config := sm.configs[num]
	duplicate := Config{Num: num, Shards: [NShards]int{}, Groups: make(map[int][]string)}
	// 深拷贝。
	for i, v := range config.Shards {
		duplicate.Shards[i] = v
	}
	for gid, servers := range config.Groups {
		// duplicate.Groups[gid] = servers // 错误，slice也需要深拷贝。
		duplicate.Groups[gid] = make([]string, len(servers))
		for i, server := range servers {
			duplicate.Groups[gid][i] = server
		}
	}
	return duplicate
}

type groupAndShards struct {
	gid    int
	shards []int
}

func (sm *ShardMaster) reallocShardsForJoin(args *JoinArgs, newConfig *Config) {
	// for i := 0; i < NShards; {
	// 	for gid := range newConfig.Groups {
	// 		if i == NShards {
	// 			break
	// 		}
	// 		newConfig.Shards[i] = gid
	// 		i++
	// 	}
	// }
	// 上面这块代码虽然能完成均匀再分配，但不满足：move as few shards as possible to achieve that goal。
	if len(sm.configs) == 1 {
		// 所有shard均未分配给任何group，这样只需把所有shard均匀分配给新增的全部group即可。
		for i := 0; i < NShards; {
			for g := range newConfig.Groups {
				newConfig.Shards[i] = g
				i = i + 1
				if i == NShards {
					break // 均匀分配完成。
				}
			}
		}
		return
	}
	shardsOfGroup := make(map[int][]int) // XXX 使用map的问题是，遍历是随机的。
	for s, g := range newConfig.Shards {
		shardsOfGroup[g] = append(shardsOfGroup[g], s)
	}
	oldGroups := []groupAndShards{}
	newGroups := []groupAndShards{}
	for g, ss := range shardsOfGroup {
		oldGroups = append(oldGroups, groupAndShards{gid: g, shards: ss}) // 这里就可以放心地用浅拷贝。
	}
	for g := range args.Servers {
		newGroups = append(newGroups, groupAndShards{gid: g, shards: []int{}})
	}
	latestConfig := sm.configs[newConfig.Num-1]
	min := NShards / len(latestConfig.Groups)
	// 因为从一开始，对shard的分配就是均匀的，所以max最多比min多1，或者和min相等。
	max := min
	if min*len(latestConfig.Groups) < NShards {
		max = min + 1
		// 将负责max个shard的group安排到oldGroups的前面，这样就可以从这些负责max个shard的group开始减少shard，添加到new group中。
		// two pointer.
		j := 0
		for i := 0; i < len(oldGroups); i++ {
			if len(oldGroups[j].shards) == max {
				j++
				continue
			}
			if len(oldGroups[i].shards) == max {
				oldGroups[i], oldGroups[j] = oldGroups[j], oldGroups[i] // swap
			}
		}
	}
	for i := 0; true; {
		// 下面的循环循环一次从oldGroups中移动shards，给newGroups中的每一个group多分配一个shard。
		for j /*, gas*/ := range newGroups {
			x := oldGroups[i].shards[0]
			oldGroups[i].shards = oldGroups[i].shards[1:]
			// gas.shards = append(gas.shards, x) // XXX 这只是改变了局部slice，而没有改变newGroups[i].shards。
			newGroups[j].shards = append(newGroups[j].shards, x)
			i = (i + 1) % len(oldGroups)
		}
		// 已经从oldGroups（并非从其中每一个group）中移动shard到newGroups中，使newGroups中每一个group增加一个shard。
		// 更新min。
		for _, gas := range oldGroups {
			if len(gas.shards) < min {
				min = len(gas.shards)
			}
		}
		if min == len(newGroups[0].shards) {
			break // 重新均匀分配shard好了。
		}
	}
	// 将分配结果写入newConfig中。
	for _, gas := range oldGroups {
		for _, s := range gas.shards {
			newConfig.Shards[s] = gas.gid
		}
	}
	for _, gas := range newGroups {
		for _, s := range gas.shards {
			newConfig.Shards[s] = gas.gid
		}
	}
}

func (sm *ShardMaster) reallocShardsForLeave(args *LeaveArgs, newConfig *Config) {
	if len(newConfig.Groups) == 0 {
		// 没有group了。
		for i := 0; i < NShards; i++ {
			newConfig.Shards[i] = 0
		}
		return
	}
	shardsOfGroup := make(map[int][]int) // XXX 使用map的问题是，遍历是随机的。
	for s, g := range newConfig.Shards {
		shardsOfGroup[g] = append(shardsOfGroup[g], s)
	}
	needReallocShards := []int{}
	for _, g := range args.GIDs {
		needReallocShards = append(needReallocShards, shardsOfGroup[g]...)
	}
	newGroups := []groupAndShards{}
	for g, ss := range shardsOfGroup {
		for _, deletedG := range args.GIDs {
			if g != deletedG {
				newGroups = append(newGroups, groupAndShards{gid: g, shards: ss}) // 这里就可以放心地用浅拷贝。
			}
		}
	}
	latestConfig := sm.configs[newConfig.Num-1]
	min := NShards / len(latestConfig.Groups)
	// 因为从一开始，对shard的分配就是均匀的，所以max最多比min多1，或者和min相等。
	if min*len(latestConfig.Groups) < NShards {
		// 将负责min个shard的group安排到newGroups的前面，这样就可以从这些负责min个shard的group开始添加被删除的group负责的shards。
		// two pointer.
		j := 0
		for i := 0; i < len(newGroups); i++ {
			if len(newGroups[j].shards) == min {
				j++
				continue
			}
			if len(newGroups[i].shards) == min {
				newGroups[i], newGroups[j] = newGroups[j], newGroups[i] // swap
			}
		}
	}
	for i := 0; i < len(needReallocShards); {
		for j /*, gas*/ := range newGroups {
			// XXX 这只是改变了局部slice，而没有改变newGroups[i].shards。
			// gas.shards = append(gas.shards, needReallocShards[i])
			newGroups[j].shards = append(newGroups[j].shards, needReallocShards[i])
			i++
			if i == len(needReallocShards) {
				break
			}
		}
	}
	// 将分配结果写入newConfig中。
	for _, gas := range newGroups {
		for _, s := range gas.shards {
			newConfig.Shards[s] = gas.gid
		}
	}
}

// The Join RPC is used by an administrator to add new replica groups.
// Its argument is a set of mappings from unique, non-zero replica group identifiers (GIDs) to lists of server names.
// The shardmaster should react by creating a new configuration that includes the new replica groups.
// The new configuration should divide the shards as evenly as possible among the full set of groups,
// and should move as few shards as possible to achieve that goal. The shardmaster should allow re-use of a GID
// if it's not part of the current configuration (i.e. a GID should be allowed to Join, then Leave, then Join again).
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("[ShardMaster %d Join] RPC handler, args = %+v", sm.me, *args)
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}

	reply.Err = ""
	reply.WrongLeader = false

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(*args)

	op := Op{Operation: "Join", Args: w.Bytes()}

	// try commit operation to raft log
	succ := sm.proposeOp(op)
	if !succ {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
	}
}

// The Leave RPC's argument is a list of GIDs of previously joined groups.
// The shardmaster should create a new configuration that does not include those groups,
// and that assigns those groups' shards to the remaining groups. The new configuration
// should divide the shards as evenly as possible among the groups, and should move as few shards
// as possible to achieve that goal.
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("[ShardMaster %d Leave] RPC handler, args = %+v", sm.me, *args)
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}

	reply.Err = ""
	reply.WrongLeader = false

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(*args)

	op := Op{Operation: "Leave", Args: w.Bytes()}

	// try commit operation to raft log
	succ := sm.proposeOp(op)
	if !succ {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("[ShardMaster %d Move] RPC handler, args = %+v", sm.me, *args)
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}

	reply.Err = ""
	reply.WrongLeader = false

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(*args)

	op := Op{Operation: "Move", Args: w.Bytes()}

	// try commit operation to raft log
	succ := sm.proposeOp(op)
	if !succ {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("[ShardMaster %d Query] RPC handler, args = %+v", sm.me, *args)
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}

	reply.Err = ""
	reply.WrongLeader = false

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(*args)

	op := Op{Operation: "Query", Args: w.Bytes()}

	// try commit operation to raft log
	succ := sm.proposeOp(op)
	if !succ {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}

	reply.Config = <-sm.queryCh
}

// 循环该请求的操作是否已经执行过。
func (sm *ShardMaster) execute(clerkID int32, reqID int64) bool {
	// 约定caller持锁调用。
	latestReqID, ok := sm.duplicateTable[clerkID]
	if ok && reqID <= latestReqID {
		return false // duplicate request，该操作已经执行过了，不再执行。
	}
	sm.duplicateTable[clerkID] = reqID // 新的request。
	return true                        // 非重复请求，执行操作。
}

// 将已commit的LogEntry按log序执行应用到状态机上。
func (sm *ShardMaster) apply() {
	// 这个线程的生存期与kv server的生存期一样长。
	var op Op
	var applyMsg raft.ApplyMsg
	for {
		select {
		case applyMsg = <-sm.applyCh:
		case <-sm.shutdown:
			return
		}

		op, _ = (applyMsg.Command).(Op) // 记得做一下类型断言，否则类型不同，即使值相同，也不相等。
		r := bytes.NewBuffer(op.Args)
		d := labgob.NewDecoder(r)
		sm.mu.Lock()
		switch op.Operation {
		case "Join":
			var args JoinArgs
			if d.Decode(&args) != nil {
				panic("decode failed")
			}
			if !sm.execute(args.ClerkID, args.ReqID) {
				break
			}
			newConfig := sm.deepCopyConfig(len(sm.configs) - 1) // 拷贝最新的那个Config，在此基础上改。
			newConfig.Num++                                     // 记得更新副本的Num。
			// 添加新的group。
			for gid, servers := range args.Servers {
				// newConfig.Groups[gid] = servers // 错误，slice也需要深拷贝。
				newConfig.Groups[gid] = make([]string, len(servers))
				for i, server := range servers {
					newConfig.Groups[gid][i] = server
				}
			}
			// 重新分配shards，要求尽可能均匀，标准是什么？
			// 看测试用例可知道，均匀的定义是Groups中各个group负责的shard数，最大值最小值之差不超过1。
			sm.reallocShardsForJoin(&args, &newConfig)
			// 将新的config插入sm.configs中。
			sm.configs = append(sm.configs, newConfig)
			DPrintf("[ShardMaster %d apply] apply Join, new config = %+v", sm.me, sm.configs[len(sm.configs)-1])
		case "Leave":
			var args LeaveArgs
			if d.Decode(&args) != nil {
				panic("decode failed")
			}
			if !sm.execute(args.ClerkID, args.ReqID) {
				break
			}
			newConfig := sm.deepCopyConfig(len(sm.configs) - 1) // 拷贝最新的那个Config，在此基础上改。
			newConfig.Num++                                     // 记得更新副本的Num。
			// 删除group。
			for _, g := range args.GIDs {
				delete(newConfig.Groups, g)
			}
			// 重新分配被删除的group负责的shards。
			sm.reallocShardsForLeave(&args, &newConfig)
			// 将新的config插入sm.configs中。
			sm.configs = append(sm.configs, newConfig)
			DPrintf("[ShardMaster %d apply] apply Leave, new config = %+v", sm.me, sm.configs[len(sm.configs)-1])
		case "Move":
			var args MoveArgs
			if d.Decode(&args) != nil {
				panic("decode failed")
			}
			if !sm.execute(args.ClerkID, args.ReqID) {
				break
			}
			newConfig := sm.deepCopyConfig(len(sm.configs) - 1) // 拷贝最新的那个Config，在此基础上改。
			newConfig.Num++                                     // 记得更新副本的Num。
			newConfig.Shards[args.Shard] = args.GID
			// 将新的config插入sm.configs中。
			sm.configs = append(sm.configs, newConfig)
			DPrintf("[ShardMaster %d apply] apply Move, new config = %+v", sm.me, sm.configs[len(sm.configs)-1])
		case "Query":
			var args QueryArgs
			if d.Decode(&args) != nil {
				panic("decode failed")
			}
			if !sm.execute(args.ClerkID, args.ReqID) {
				break
			}
			config := Config{}
			// 注意下面的做法是错误的，因为这样只是浅拷贝，虽然现在程序都运行在同一个进程中，没有问题。
			// 但实际应用时就会出问题。
			// config = sm.configs[args.Num]
			fmt.Printf("%d, %+v\n", args.Num, sm.configs)
			if args.Num == -1 || args.Num >= len(sm.configs) {
				config = sm.deepCopyConfig(len(sm.configs) - 1) // 返回最新的那个Config。
			} else {
				config = sm.deepCopyConfig(args.Num)
			}
			DPrintf("[ShardMaster %d apply] apply Query, reply.Config = %+v", sm.me, config)
			// 不要持锁进行可能阻塞的操作。
			go func() {
				sm.queryCh <- config
			}()
		}
		sm.mu.Unlock()

		sm.commandIndex = applyMsg.CommandIndex
		sm.op = op
		sm.cond.Broadcast()
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.shutdown)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	// sm.configs[0].Shards是一个数组而不是slice，已分配好内存可容纳NShards个int，且默认初始化为0，表示group 0。
	// sm.Num也默认初始化为0。

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.commandIndex = -1
	sm.cond = sync.NewCond(new(sync.Mutex))
	sm.duplicateTable = make(map[int32]int64)
	sm.queryCh = make(chan Config)

	sm.shutdown = make(chan struct{})

	// single goroutine which keeps reading applyCh
	go sm.apply()

	return sm
}
