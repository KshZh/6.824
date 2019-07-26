package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// ApplyMsg ...
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	follower int = iota
	candidate
	leader
)

var stateStr = [3]string{"follower", "candidate", "leader"}

// Raft ...
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	logs        []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// 以下是自定义字段
	nPeers                int // 缓存len(rf.peers)，这样不用每次遍历peers slice都重新计算
	applyCh               chan ApplyMsg
	state                 int           // 该raft server所处的状态
	shutdown              chan struct{} // 关闭与该raft server相关的所有线程
	electionEventLoopDone chan struct{} // 关闭发送并等待RequestVote RPC的线程
	// 注意并不是关闭electionEventLoop()，electionEventLoop()退出的唯一条件是shutdown被关闭，或candidate被选举为leader
	leaderEventLoopDone chan struct{} // 当leader退回follower时，关闭leaderEventLoop()及相关线程
	timeout             *time.Timer   // 计时器
	applyReq            chan struct{} // 发送server当前的lastApplied给writeApplyCh()线程
}

// LogEntry ...
// 类型名大写开头、驼峰
type LogEntry struct {
	Index   int // 非必要，只是调试会容易
	Term    int
	Command interface{}
}

// GetState ...
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.state == leader
	// Your code here (2A).
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// 该函数有caller持锁调用，避免下面三个`e.Encode()`调用被调度，造成要持久化的状态被破坏。
	w := new(bytes.Buffer)
	// Will write to w, 注意传递的是指针。
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	rf.persister.SaveRaftState(w.Bytes())
	// DPrintf("PPPP[%s %d/%d]: persist succ, votedFor=%d, logs=%v",
	// 	stateStr[rf.state], rf.me, rf.currentTerm, rf.votedFor, rf.logs)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	// Will read from r, 注意传递的是指针。
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	// Decode记得传入地址，这样才能写入实参本身，而不是实参的副本
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		DPrintf("PPPP[%s %d/%d]: readPersist failed", stateStr[rf.state], rf.me, rf.currentTerm)
		panic("readPersist failed")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		DPrintf("PPPP[%s %d/%d]: readPersist succ, votedFor=%d, logs=%v",
			stateStr[rf.state], rf.me, rf.currentTerm, rf.votedFor, rf.logs)
	}
}

// RequestVoteArgs ...
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate’s term
	CandidateID int // candidate requesting vote
	// 以下两个字段用于实现Election restriction:
	// Raft uses the voting process to prevent a candidate from
	// winning an election unless its log contains all committed entries.
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// RequestVoteReply ...
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote ...
// example RequestVote RPC handler.
//
// 要明确，只有candidate会发送RequestVote RPC，所以在RequestVote RPC的处理函数中，我们可以认为发送者是一个candidate，
// 然后从这个角度来考虑问题。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	} else if args.Term == rf.currentTerm && rf.state == leader {
		// leader不会投票给同term的candidate。
		// 这种情况在一次选举中，多个candidate，有一个胜出后，这是leader就可能收到其它candidate的RequestVote RPC。
		return
	}
	// • If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		DPrintf("[%s %d/%d]: come across newer term %d, back to follower",
			stateStr[rf.state], rf.me, rf.currentTerm, args.Term)
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.currentTerm = args.Term
		if rf.state == follower {
			rf.votedFor = -1
		} else {
			rf.becomeFollower()
		}
		rf.persist()
	}
	// 只重置follower计时器，同term的candidate在发起选举时就已经重置计时器了。

	// Figure 2, rules for followers:
	// If election timeout elapses without receiving AppendEntries RPC
	// from current leader or granting vote to candidate: convert to candidate
	// 即仅当当前follower把票投了出去才重置自己的计时器，
	// 否则，设想这样一种情况，candidateA(term 11)有较旧的logs，followerB(term 10)有较新的logs，
	// followerB每次收到candidateA的RequestVote RPC就重置自己的timeout计时器，
	// 由此，candidateA不可能成为leader，但followerB也永远无法timeout成为candidate参与选举。
	// if rf.state == follower {
	// 	rf.resetTimeout()
	// }

	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	// 注意，这一段的两个条件分别体现了两个Election restriction，
	// 一个是：
	// Each server will vote for at most one candidate in a
	// given term, on a first-come-first-served basis.(Election Safety)
	// 另一个是：
	// Raft uses the voting process to prevent a candidate from
	// winning an election unless its log contains all committed entries.
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID { // 这个条件隐含表示，如果当前server也是candidate，它不会投给请求的candidate。
		nLog := len(rf.logs)
		lastLogTerm := rf.logs[nLog-1].Term
		// up-to-date的定义如下：
		// If the logs have last entries with different terms, then
		// the log with the later term is more up-to-date.
		// If the logs end with the same term, then whichever log is longer
		// is more up-to-date.
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && nLog <= args.LastLogIndex+1) {
			reply.VoteGranted = true
			// 记得更新voteFor，一个follower在一个term中只会投给一个candidate，先到先得。
			// 这个规则能够有效防止一个term中选出两个leader。
			rf.votedFor = args.CandidateID
			rf.resetTimeout() // 经过两个if判断，能进入到这里的一定是一个follower，投票成功，重置它的计时器。
			rf.persist()
			DPrintf("[%s %d/%d]: vote for candidate %d", stateStr[rf.state], rf.me, rf.currentTerm, rf.votedFor)
		}
	}
}

// AppendEntriesArgs ...
type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

// AppendEntriesReply ...
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// 论文中提到的优化，即快速调整leader的对于自己的nextIndex，因为2C有要求，所以必须要实现它。
	// 可参考https://thesquareplanet.com/blog/students-guide-to-raft/
	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries RPC handler...
// 明确只有leader才发送AppendEntries RPC，所以在AppendEntries RPC的处理函数中，我们可以认为发送者是一个leader，
// 然后从这个角度考虑问题。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false

	// leader收到AppendEntries RPC该怎么办？
	// 首先，这两个leader的term肯定是不同的，因为前面我们有election restriction保证一个term只能有一个leader，
	// 或者没有leader，这样只需让较小term的leader转换为follower即可。

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}
	// candidate: If AppendEntries RPC received from new leader: convert to follower
	// 第二个条件表示同term的candidate看到胜出者leader，就该退出选举，转换回follower。
	if args.Term > rf.currentTerm || (rf.state == candidate && args.Term == rf.currentTerm) {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		DPrintf("[%s %d/%d]: AppendEntries RPC handler, come across newer term %d, back to follower\n",
			stateStr[rf.state], rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		if rf.state == follower {
			rf.votedFor = -1
		} else {
			rf.becomeFollower()
		}
		rf.persist()
	}
	// 重置计时器，因为发现了term至少等于当前server的leader，防止server再发起选举。
	rf.resetTimeout()

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)

	// consistency check，这样可以保证Log Matching Property:
	// if two logs contain an entry with the same
	// index and term, then the logs are identical in all entries
	// up through the given index. §5.3
	// XXX 每一次，即便仅仅就是heartbeat，没有携带Entries，也要进行AppendEntries consistency check
	// 确保自己的log与leader相一致，且做到不一致时及时通知leader更新自己的log
	nLogs := len(rf.logs)
	// 短路操作，确保不会数组访问越界，下面是没有优化时，一次只反应一个LogEntry不一致给leader。
	// if nLogs <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	return
	// }
	// 论文中提到的优化，即快速调整leader的nextIndex[server]。
	// https://thesquareplanet.com/blog/students-guide-to-raft/
	if nLogs <= args.PrevLogIndex {
		// If a follower does not have prevLogIndex in its log, it should return with
		reply.ConflictIndex = nLogs
		reply.ConflictTerm = -1
		return
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// If a follower does have prevLogIndex in its log, but the term does not match, it should return
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
		// and then search its log for the first index whose entry has term equal to conflictTerm.
		for i := 0; i < nLogs; i++ {
			if rf.logs[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		return
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it (§5.3)
	// 4. Append any new entries not already in the log
	nEntries := len(args.Entries)
	if nEntries > 0 {
		appendIndex := args.PrevLogIndex + 1
		for i, e := range args.Entries {
			if appendIndex+i >= nLogs {
				rf.logs = append(rf.logs, args.Entries[i:]...) // 无冲突，但logs中没有，append。
				break
			} else if e.Term != rf.logs[appendIndex+i].Term {
				rf.logs = append(rf.logs[:appendIndex+i], args.Entries[i:]...) // 有冲突，截断后append。
				break
			} else {
				// e.Term == rf.logs[appendIndex+i].Term
				// 同一位置相同term，可能当前RPC请求是之前处理过的RPC请求的leader重发的副本，
				// 比如网络延迟，leader先后发送了两个携带x的参数相同的RPC，此时因为是同一个LogEntry故不需要做什么。
			}
		}

		// 注意不能没有判断，而直接截断后append，
		// This is because we could be receiving an outdated AppendEntries RPC from the leader,
		// and truncating the log would mean “taking back” entries that we may have already
		// told the leader that we have in our log.
		// rf.logs = append(rf.logs[:appendIndex], args.Entries...) // 总是截断后插入。

		rf.persist()
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// 其中的min操作是必要的，不能直接设置commitIndex=leaderCommit，这样可能导致commitIndex>=len(logs)，导致数组越界异常，
	// 也不能直接设置commitIndex=index of last new entry，这样若leaderCommit<index of last new entry，
	// 就导致该follower擅自commit了leader还未commit的LogEntry，并且这些LogEntry还可能与leader不一样。
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1) // 这里不能再用局部变量nlogs了，因为它可能存放的是旧值，除非给它赋新值。
		DPrintf("FFFF[%s %d/%d]: commitIndex have updated to %d, have commit %v\n",
			stateStr[rf.state], rf.me, rf.currentTerm, rf.commitIndex, rf.logs[:rf.commitIndex+1])
	}

	// All Servers:
	// • If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
	if rf.commitIndex > rf.lastApplied {
		// 应用到状态机。
		rf.applyReq <- struct{}{}
	}

	reply.Success = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start ...
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2B).
	// 只有leader才能处理客户端的请求。
	if rf.state != leader {
		return -1, -1, false
	}

	// Leaders:
	// • If command received from client: append entry to local log,
	// respond after entry applied to state machine (§5.3)
	rf.logs = append(rf.logs, LogEntry{Index: len(rf.logs), Term: rf.currentTerm, Command: command})
	rf.persist()
	DPrintf("[%s %d/%d]: receive client request %v, my logs %v", stateStr[rf.state], rf.me, rf.currentTerm, command, rf.logs)

	index := len(rf.logs) - 1
	term := rf.currentTerm
	isLeader := true

	// 我们不单独拿出leaderEventLoop之外的线程进行agreement，而是利用leaderEventLoop进行heartbeat的同时进行agreement。
	// 这很关键，因为只有leaderEventLoop这个线程在做这个事情，可以避免许多隐晦的错误，也不需要关注繁琐的同步问题，容易管理，而且“物尽其用”。
	// go rf.startAgreement(command)

	return index, term, isLeader
}

// Kill ...
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// 如果没有这么做，这次测试中的几个raft server的相关事件循环线程就还会一直运行，
	// 在这里，Test2A1创建并运行的几个Raft server就会干扰到Test2A2。因为这里它们属于同一个进程，只是对真实情况的模拟。
	close(rf.shutdown)
}

func (rf *Raft) resetTimeout() {
	// 由于sync.Mutex是不可重入的，所以不要在这里面加锁，不然容易死锁，要不要加锁由caller负责。

	// To prevent a timer created with NewTimer from firing after a call to Stop,
	// check the return value and drain the channel.
	// 对于文档中的这句话，一个问题是为什么调用了Stop()后，timer还会fire？
	// 注意，Stop()返回false有两种情况，一是该timer已经被Stop()过了，二是该timer过期了，其channel中含有值。
	// 其中Stop()返回false的第二种情况导致文档所述的情况，显然这种情况**违背了Stop()字面上的语义**，容易使用该函数的
	// 人写出有隐晦bug的代码，也给阅读代码造成障碍。

	// 我们期望处理的应该是第二种情况。
	if !rf.timeout.Stop() && len(rf.timeout.C) > 0 {
		<-rf.timeout.C
	}
	rf.timeout.Reset(getRandomTimeout())
}

func (rf *Raft) becomeFollower() {
	// 约定caller要在持锁时才能调用becomeFollower()，因为它会读写共享变量。
	if rf.state == follower {
		return
	}

	if rf.state == leader {
		// 由于leaderEventLoopDone决定多个线程的提出，只发送一个值只会让一个线程退出。
		// 所以直接close该channel，再新建一个，这样这段代码就不依赖于等待leaderEventLoopDone的线程数了。
		// rf.leaderEventLoopDone <- struct{}{}
		close(rf.leaderEventLoopDone)
		rf.leaderEventLoopDone = make(chan struct{})
		// 重新开启election timeout事件循环。
		rf.resetTimeout()
		go rf.electionEventLoop()
	} else if rf.state == candidate {
		// 关闭发送并等待RequestVote RPC的线程。
		close(rf.electionEventLoopDone)
		rf.electionEventLoopDone = make(chan struct{})
	}
	rf.state = follower
	// 现在，当前follower在**当前term中**有一张票，可以投给candidate，先到先得。
	rf.votedFor = -1
}

func (rf *Raft) becomeLeader() {
	// 约定caller要在持锁时才能调用becomeLeader()，因为它会读写共享变量。
	if rf.state == leader {
		rf.mu.Unlock()
		panic("a leader call becomeLeader()")
	}

	rf.state = leader
	// When a leader first comes to power, it initializes
	// all nextIndex values to the index just after the last one in its log.
	for i := 0; i < rf.nPeers; i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.logs)
	}
	// 上面一些初始化条件准备好了后，才开启leaderEventLoop()。
	// 注意，不能把下面的语句放在该函数的第一行，因为一些条件如nextIndex没有初始化好，就会出现一些隐晦的错误。
	go rf.leaderEventLoop()

	// leader不需要运行ElectionEventLoop。
	close(rf.electionEventLoopDone)
	rf.electionEventLoopDone = make(chan struct{})
	DPrintf("[%s %d/%d]: win the election, become leader", stateStr[rf.state], rf.me, rf.currentTerm)
}

func (rf *Raft) electionEventLoop() {
	peersVoteForMe := make(chan struct{})
	nVote := 0 // 统计当前candidate所获投票数
	for {
		select {
		case <-rf.timeout.C:
			nVote = 1
			rf.mu.Lock()
			rf.state = candidate // modify state
			rf.currentTerm++     // Increment currentTerm
			rf.votedFor = rf.me  // Vote for self
			rf.resetTimeout()    // Reset election timer
			rf.persist()
			args := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me,
				LastLogIndex: len(rf.logs) - 1, LastLogTerm: rf.logs[len(rf.logs)-1].Term}
			rf.mu.Unlock()
			DPrintf("[%s %d/%d]: timeout, start the election", stateStr[rf.state], rf.me, rf.currentTerm)
			// Send RequestVote RPCs to all other servers
			for i := 0; i < rf.nPeers; i++ {
				if i == rf.me {
					continue
				}
				// 发给所有peer的args都是一样的，只需创建一个，然后传递指针即可。
				go rf.sendAndWaitRequestVoteRPC(i, &args, peersVoteForMe)
			}
		case <-peersVoteForMe:
			// 不需要加锁保护nVote，因为各个发起RPC的线程并不直接修改nVote，而是发送信号统一由这里串行操作nVote。
			nVote++
			if nVote > rf.nPeers/2 {
				// If votes received from majority of servers: become leader
				rf.becomeLeader() // 注意到，becomeLeader()只会在这里被调用。
				return            // 退出electionEventLoop()
			}
		case <-rf.shutdown:
			return
		}
	}
}

func (rf *Raft) sendAndWaitRequestVoteRPC(server int, args *RequestVoteArgs, peersVoteForMe chan struct{}) {
	reply := &RequestVoteReply{}

	// 是否处理RPC响应的规则：不同term不处理，非处理角色不处理。

	// 注意下面的if判断存在竞争条件，应该在持锁时访问共享变量。
	// if rf.sendRequestVote(server, &args, &reply) && rf.currentTerm == args.Term && rf.state == candidate {
	if rf.sendRequestVote(server, args, reply) {
		// 注意语句顺序，并且注意不要在阻塞操作rf.sendRequestVote()前加锁，要在之后加锁。
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !(rf.currentTerm == args.Term && rf.state == candidate) {
			return
		}
		if reply.Term > rf.currentTerm {
			DPrintf("[%s %d/%d]: come across newer term %d, back to follower",
				stateStr[rf.state], rf.me, rf.currentTerm, reply.Term)
			rf.currentTerm = reply.Term // 更新自己的term
			rf.becomeFollower()
			rf.persist()
		} else if reply.VoteGranted {
			// 不要持锁进行可能阻塞的操作，考虑一种情况：
			// 当`case <-peersVoteForMe:`正在执行`rf.becomeLeader()`时，
			// 下面的写channel就会阻塞，此时当前线程仍持有锁，而becomeLeader()要求得到锁之后再能继续执行，
			// 于是就死锁了。因此，要在写channel之前先解除当前线程持有的锁。
			// 注意看下面日志的时间，candidate2虽然在31秒获得的多数票，直到32秒follower0仍没成为leader，因为candidate2死锁了。
			// 2019/05/07 13:47:31 [candidate 2/1]: timeout, start the election
			// 2019/05/07 13:47:31 [follower  0/0]: RequestVote RPC handler, come across newer term 1, back to follower
			// 2019/05/07 13:47:31 [follower  0/1]: vote for candidate 2
			// 2019/05/07 13:47:31 [follower  1/0]: RequestVote RPC handler, come across newer term 1, back to follower
			// 2019/05/07 13:47:31 [follower  1/1]: vote for candidate 2
			// 2019/05/07 13:47:32 [candidate 0/2]: timeout, start the election
			rf.mu.Unlock()
			select {
			case peersVoteForMe <- struct{}{}:
			case <-rf.electionEventLoopDone: // 确保该线程最终会退出。
			case <-rf.shutdown:
			}
			rf.mu.Lock() // 因为前面写了`defer rf.mu.Unlock()`。
		}
	}
}

func (rf *Raft) leaderEventLoop() {
	DPrintf("[%s %d/%d]: start leaderEventLoop", stateStr[rf.state], rf.me, rf.currentTerm)
	// 因为ticker不会在初始化时就触发一次，所以为了尽快让新进的leader马上发出heartbeat，就不用ticker了。
	// 记得要间隔一段时间，如果忘了，那么网络中就会充斥着无数的heartbeat，
	// peer的AppendEntries handler就会连续不断地被调用，导致其它事务无法进行。
	for {
		for i := 0; i < rf.nPeers; i++ {
			if i == rf.me {
				continue
			}
			go rf.sendAndWaitAppendEntriesRPC(i)
		}

		select {
		case <-rf.leaderEventLoopDone:
			return
		case <-rf.shutdown:
			return
		case <-time.After(HeartBeatIntervalMs * time.Millisecond):
			break
		}
	}
}

func (rf *Raft) sendAndWaitAppendEntriesRPC(server int) {
	// 注意，由于可能同时有多个sendAndWaitAppendEntriesRPC()同时执行，所以读写共享变量时，必须上锁。
	rf.mu.Lock()
	if rf.state != leader {
		// panic("Not a leader but sendAndWaitAppendEntriesRPC()")
		rf.mu.Unlock() // 记得解锁，不然会死锁。
		return
	}
	// 因为所有Raft server的log在0处都有同样的哨兵，所以有这样一个不变量：rf.nextIndex[i] >= 1。
	if rf.nextIndex[server] < 1 {
		panic("break invariant: rf.nextIndex[i] >= 1")
	}
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1, PrevLogTerm: rf.logs[rf.nextIndex[server]-1].Term,
		Entries: nil, LeaderCommit: rf.commitIndex}
	if rf.nextIndex[server] < len(rf.logs) {
		args.Entries = append(args.Entries, rf.logs[rf.nextIndex[server]:]...) // "..."类似于python中的序列拆包
		DPrintf("[%s %d/%d]: args.Entries=%v for peer %d", stateStr[rf.state], rf.me, rf.currentTerm, args.Entries, server)
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	// 有竞争条件的代码：
	// if rf.sendAppendEntries(server, &args, &reply) && args.Term == rf.currentTerm && rf.state == leader
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 是否处理RPC响应的规则：不同term不处理，非处理角色不处理
		if !(args.Term == rf.currentTerm && rf.state == leader) {
			return
		}
		if !reply.Success && reply.Term > rf.currentTerm {
			// 该leader遇到有更大的term的leader。
			DPrintf("[%s %d/%d]: come across newer term %d，back to follower",
				stateStr[rf.state], rf.me, rf.currentTerm, reply.Term)
			rf.currentTerm = reply.Term // 紧跟新leader的term。
			rf.becomeFollower()
			rf.persist()
		} else if len(args.Entries) > 0 && reply.Success {
			// 此次RPC是log agreement且成功了。
			DPrintf("[%s %d/%d]: replicate LogEntry %v to peer %d succ",
				stateStr[rf.state], rf.me, rf.currentTerm, args.Entries, server)
			// 注意这个判断，由于网络延迟、崩溃等问题，同一个RPC发送多次，响应多次，
			// 如果一个RPC响应已经处理，对于该RPC响应的副本，不再处理。
			if rf.matchIndex[server] == args.PrevLogIndex+1+len(args.Entries)-1 {
				return
			}

			// 注意，不能写成`rf.nextIndex[server] += len(args.Entries)`，不然可能多加或错加。
			// 考虑多个携带LogEntry的RPC请求相继发送，但乱序返回响应的情况。
			rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			newCommitIndex := rf.computeCommitIndex(server)
			if newCommitIndex != rf.commitIndex {
				rf.commitIndex = newCommitIndex
				// 不要持锁执行可能阻塞的操作！
				// 如果代码执行顺序重要的话，就只能先解锁，而不要另开线程。
				go func() {
					rf.applyReq <- struct{}{}
				}()
				DPrintf("LLLL[%s %d/%d] update commitIndex to %d",
					stateStr[rf.state], rf.me, rf.currentTerm, newCommitIndex)
			}
		} else {
			// 此次RPC是不带LogEntry的log agreement或heartbeat，
			// 无论哪一种，consistency check失败了，也就是follower与leader的log不一致，都需要回退nextIndex。
			// logs不一致，回退nextIndex，然后简单地退出，留待leader下一轮AppendEntries RPC。

			// 下面这条语句是未优化时，follower一次只反应一个LogEntry不一致。
			// rf.nextIndex[server]--
			// 下面是优化后：即快速调整根据reply快速调整rf.nextIndex[server]
			// https://thesquareplanet.com/blog/students-guide-to-raft/
			if reply.ConflictTerm == -1 {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				var i int
				for i = len(rf.logs) - 1; i >= 0; i-- { // 要找最后一个，可以直接从后往前搜索第一个。
					if rf.logs[i].Term == reply.ConflictTerm {
						rf.nextIndex[server] = i + 1
						break
					}
				}
				if i == -1 {
					rf.nextIndex[server] = reply.ConflictIndex
				}
			}
			// 对于这个快速反馈算法，对于follower应该怎么设置reply.ConflictIndex和reply.ConflictTerm，
			// 其实可以从leader端对各种情况的处理来入手。这些情况包括，
			// a) follower没有RPC携带的LogEntry，即follower的log更短，leader直接将nextIndex[i]设置为follower的log的长度，这样下一次follower的log就包含args.PrevLogIndex了；
			// b) follower的log在args.PrevLogIndex冲突，若leader的log中包含termF的LogEntry，则leader将nextIndex[i]设置为其log中最后一个为termF的LogEntry的下标+1，这样下次args.PrevLogTerm就等于termF了；
			// c) 若leader的log不包含termF的LogEntry，则leader将nextIndex[i]设置为follower的log中第一个termF的LogEntry的下标，这样下一个args.PrevLogIndex直接跳过这些leader的log中没有的LogEntry了。
		}
	} else {
		// !rf.sendAppendEntries(server, &args, &reply)
		// 进入这里说明RPC失败，只需简单地退出，等待leader下一轮heartbeat操作，一直无限尝试即可。
	}
}

func (rf *Raft) computeCommitIndex(follower int) int {
	for i := rf.matchIndex[follower]; i >= rf.commitIndex; i-- {
		if rf.logs[i].Term != rf.currentTerm {
			// Figure 8 illustrates a situation where an old log entry is stored on a majority of servers,
			// yet can still be overwritten by a future leader.
			// 在Figure 8(c)中，此时S1的term是4，即使它把2复制到了majority上，它也不能直接commit它，
			// 若commit了，然后crash了，S5接着被选举为leader（凭借S2, S3, S4的投票），
			// 此时已经commit的2被覆盖了，不在log中了，但是2却又已经应用到状态机上了，raft算法失效了。

			// To eliminate problems like the one in Figure 8,
			// Raft never commits log entries from previous terms by counting replicas.
			// Only log entries from the leader’s current term are committed by counting replicas;
			// once an entry from the current term has been committed in this way,
			// then all prior entries are committed indirectly because of the Log Matching Property.
			break
		}

		count := 1 // leader已经agree了。
		for j := 0; j < len(rf.matchIndex); j++ {
			if j != rf.me && rf.matchIndex[j] >= i {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			return i // 返回新的commitIndex。
		}
	}
	return rf.commitIndex // 返回原commitIndex。
}

func (rf *Raft) responToClient() {
	// 注意，Client的指代是相对的、灵活的，在同一个服务端程序中，层A请求层B，层A就是层B的Client。
	var start, nToApply int
	var toApply []LogEntry
	for {
		select {
		case <-rf.applyReq:
		case <-rf.shutdown:
			return
		}
		rf.mu.Lock()
		// 为了计算出从哪个LogEntry开始apply，我们需要先使用完旧的rf.lastApplied再更新rf.lastApplied，
		// 而写rf.applyReq的代码则要先更新rf.commitIndex。
		start = rf.lastApplied + 1
		nToApply = rf.commitIndex - rf.lastApplied

		toApply = make([]LogEntry, nToApply)
		copy(toApply, rf.logs[start:start+nToApply]) // 拷贝到局部变量，减小临界区大小。
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for i := 0; i < nToApply; i++ {
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: toApply[i].Command, CommandIndex: start + i}
		}
	}
}

// Make ...
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 这里是每个raft server的启动函数，单线程，可以不用加锁。
	rf.currentTerm = 0
	rf.votedFor = -1

	// 这里采用一个哨兵logEntry，所有server的Raft的logs都有同一个哨兵，
	// 这样带来的好处是不用检查len(rf.logs)==0，否则rf.logs[len(rf.logs)-1]就会出错。
	// 而且对客户端来说，log entry的index是1-based的，而不是0-based，这样处理客户端参数也会方便一点，不用转换。
	rf.logs = make([]LogEntry, 1)
	rf.logs[0] = LogEntry{Index: 0, Term: 0, Command: -1} // 哨兵

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nPeers = len(rf.peers)

	rf.nextIndex = make([]int, rf.nPeers)
	rf.matchIndex = make([]int, rf.nPeers)
	for i := 0; i < rf.nPeers; i++ {
		rf.nextIndex[i] = 1
		// 所有server的Raft都有一个哨兵，所以matchIndex是0。
		rf.matchIndex[i] = 0
	}

	// 自定义字段
	rf.applyCh = applyCh
	rf.state = follower
	rf.shutdown = make(chan struct{})
	rf.electionEventLoopDone = make(chan struct{})
	rf.leaderEventLoopDone = make(chan struct{})
	rf.timeout = time.NewTimer(getRandomTimeout())
	rf.applyReq = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 专门串行写applyCh，保证写入的LogEntry不会乱序，而且避免其它线程阻塞。
	go rf.responToClient()

	// 对每一个raft peer，初始时都是follower，各开一个线程进行timeout事件循环。
	go rf.electionEventLoop()

	return rf
}

// 就像C中的宏常量，不要使用无法表达语义的纯数字。

// HeartBeatIntervalMs ...
const HeartBeatIntervalMs = 100

// MinElectionTimeoutMs ...
const MinElectionTimeoutMs = 300

// MaxIncrementMs ...
const MaxIncrementMs = 150

// 经过测试，同一个进程，每次调用都是不同的数，而不是同一个数。
func getRandomTimeout() time.Duration {
	return time.Duration(rand.Intn(MaxIncrementMs)+MinElectionTimeoutMs) * time.Millisecond
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
