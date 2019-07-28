1. 对于raft结构，论文中Figure 2提及的字段，都要加上，另外再添加一些自己定义的字段。对于RPC handler的执行流程，基本按照Figure 2的指导来写，再添加一些运行测试时遇到的问题的解决代码。

2. 重视和理解论文中Figure 2提及的字段的语义(semantic)，如果不知道这些字段代表什么，作用是什么，那么怎么阅读理解论文，在实现时怎么知道什么情况下要更新这些字段，为什么更新。注意commitIndex: index of highest log entry known to be committed，应该理解为，已知的已commit的最高的log entry的下标，而不是，已知的即将被commit的最高的log entry的下标。

3. 我的实现中主要就两个线程，也就是两个事件循环：
	- electionEventLoop()，它监测follower的timeout，在timeout时发起一次或多次election，期间收集其它peers的投票，结果就是论文中所提及的三个结果：
		> (a) it wins the election, become leader (b) another server establishes itself as leader, thus back to follower, or (c) a period of time goes by with no winner, and start a new election again.
		
		当一个candidate成为leader时，这个事件循环及相关子线程会被关闭，因为leader不需要；
	- leaderEventLoop()，它的任务也就是论文中描述的发送heartbeat，以及log replication。当一个leader转变为follower时，该事件循环及相关子线程会被关闭，因为只有leader才能开启这个事件循环。
	
4. 对于Start()函数，一开始每次调用Start()都会独自开一些线程向peers发送AppendEntries RPC进行log replication，然而这样和持续周期性发送heartbeat的线程leaderEventLoop()混杂在一起，使得复杂度陡增，问题不断。根本的解决方法是Start()只需要简单地把客户端请求的LogEntry添加到leader的logs中即可，它的任务也就完成了。然后只使用leaderEventLoop()，使得该线程在起到重置peers的election timeout的同时，也顺带进行log replication，这可以通过利用nextIndex[i]来进行，具体操作论文中5.3节也有详细指导。这样，因为只有leaderEventLoop()这个线程在调度子线程发送并等待AppendEntries RPC，可以避免许多隐晦的错误，也不需要过多关注繁琐的同步问题，而且“物尽其用”。

5. term作为raft算法中的时间量度，不管server处于什么状态(follower/candidate/leader)，只要RequestVote/AppendEntries RPC的sender或receiver发现了更新的term，它都必须跟进这个更新的term，并转换回follower。Figure 2 All Servers:

   > if RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)

6. Figure 2中，RequestVote RPC handler的实现指导：
   > 2\. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

   注意，这一段的两个条件分别体现了两个Election restriction：
   1. Each server will vote for at most one candidate in a given term, on a first-come-first-served basis.
   2. Raft uses the voting process to prevent a candidate from winning an election unless its log contains all committed entries.

   up-to-date的定义是：
   > If the logs have last entries with different terms, then the log with the later term is more up-to-date.
   > If the logs end with the same term, then whichever log is longer is more up-to-date.

7. 不必担心这样一种情况：网络不可达（分区），导致某raft server X一直在选举，它的term按照论文的指导，将会一直增大，当它网络修复时，X由于term最大，成为leader。实际上，当X网络可达时，其它有较小term的raft server（包括leader）首先会观察到这个更大的term，跟进这个更新的term，从而先转换为follower，之后会进入选举，而由于Election restriction，X不可能成为leader，更大的可能是刚刚退位的leader（或其它包含所有已commit的LogEntry的raft server）在这次选举中再次成为leader，之后新leader复制自己的log给X，让X跟上来。这里也体现了Election restriction的重要性。

8. Figure 2, rules for followers:
	> If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate.

	即仅当当前follower把票投了出去才重置自己的计时器，否则，设想这样一种活锁的情况，candidateA(term 11)有较旧的logs，followerB(term 10)有较新的logs，followerB每次收到candidateA的RequestVote RPC就重置自己的timeout计时器，即使由于Election restriction，followerB并没有投票给candidateA，这样就会导致，candidateA由于Election restriction而不可能成为leader，只能反复重新进入选举，但followerB也永远无法timeout成为candidate参与选举。

	总结一下，一个server应该重置其timeout计时器，仅当：
	1. you get an AppendEntries RPC from the current leader (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer);
	2. you are starting an election;
	3. you grant a vote to another peer.

9. 对于RPC响应是否处理的通用处理规则：
	> compare the current term with the term you sent in your original RPC. If the two are different, drop the reply and return. Only if the two terms are the same should you continue processing the reply. 

	其实还有一个规则是，如果收到RPC响应后，server已经不是应当处理响应的角色了，也不应该处理响应。如candidate发送RequestVote RPC，收到响应时已经不是candidate了，那么继续处理响应是无意义的。

	总结是否处理RPC响应的规则：不同term不处理，非处理角色不处理。

10. raft算法中，server角色的转变必然伴随着term的更新吗？不，如一个选举中，多个同term的candidate，未被选上的candidate转变回follower，其中，它们的term并没有更新。

11. 
    > **To eliminate problems like the one in Figure 8, Raft never commits log entries from previous terms by counting replicas. Only log entries from the leader’s current term are committed by counting replicas; once an entry from the current term has been committed in this way, then all prior entries are committed indirectly because of the Log Matching Property**. There are some situations where a leader could safely conclude that an older log entry is committed (for example, if that entry is stored on every server), but Raft takes a more conservative approach for simplicity.

    其中Log Matching Property: if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index. §5.3

    再注意到Figure 2中，rules for leaders:
    > If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).

    注意条件`log[N].term == currentTerm`，即不主动commit旧term的LogEntry，而是依赖Log Matching Property来间接commit它。

    为什么不能直接commit旧term的LogEntry呢？看Figure 8，Figure 8 illustrates a situation where an old log entry is stored on a majority of servers, yet can still be overwritten by a future leader。在Figure 8(c)中，此时S1的term是4，即使它把LogEntry2复制到了majority上，它也不能直接commit它，若commit了，然后crash了，S5接着被选举为leader（凭借S2, S3, S4的投票），此时已经commit的LogEntry2会被覆盖，log没有LogEntry2的记录，但是LogEntry2却又已经应用到状态机上了。在这种情况下，raft算法失效了。 

12. Figure 2对AppendEntries RPC handler的实现指导：
    > 2\. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)

    这个也就是consistency check，这样可以保证Log Matching Property。

    注意，leader的每一个AppendEntries RPC，即便仅仅就是heartbeat，没有携带LogEntry，也要进行AppendEntries consistency check，即leader在RPC请求中包含PrevLogIndex和PrevLogTerm，接收者每一次收到AppendEntries RPC，都要进行consistency check，确保自己的log与leader相一致，且做到不一致时及时通知leader更新自己的log。

13. 
    > In Raft, the leader handles inconsistencies by forcing the followers’ logs to duplicate its own. This means that conflicting entries in follower logs will be overwritten with entries from the leader’s log. Section 5.4 will show that this is safe when coupled with one more restriction.（在Election restriction的帮助下，这个恢复不一致的做法是安全有效的）
    > 
    > **To bring a follower’s log into consistency with its own, the leader must find the latest log entry where the two logs agree, delete any entries in the follower’s log after that point, and send the follower all of the leader’s entries after that point**. All of these actions happen in response to the consistency check performed by AppendEntries RPCs. **The leader maintains a nextIndex for each follower, which is the index of the next log entry the leader will send to that follower. When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log**(11 in Figure 7). If a follower’s log is inconsistent with the leader’s, the AppendEntries consistency check will fail in the next AppendEntries RPC. After a rejection, the leader **decrements nextIndex** and retries the AppendEntries RPC. Eventually nextIndex will reach a point where the leader and follower logs match. When this happens, AppendEntries will succeed, which removes any conflicting entries in the follower’s log and appends entries from the leader’s log (if any). Once AppendEntries succeeds, the follower’s log is consistent with the leader’s, and it will remain that way for the rest of the term.

14. [guide](https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations)中描述了快速回溯nextIndex的算法，对leader：

    1. follower没有args.Entries，即follower的log更短，leader直接将nextIndex[i]设置为len(f.logs)，这样下一次follower的log就包含args.PrevLogIndex了；
    2. follower的log在args.PrevLogIndex冲突，若leader的log中包含termF=f.logs[args.PrevLogIndex].Term的LogEntry，则leader将nextIndex[i]设置为其log中最后一个为termF的LogEntry的下标+1，这样下次args.PrevLogTerm就等于termF了；
    3. 若leader的log不包含termF的LogEntry，则leader将nextIndex[i]设置为follower的log中第一个termF的LogEntry的下标，这样下一个args.PrevLogIndex直接跳过这些leader的log中没有的LogEntry了。

	这样出现不一致的follower在AppendEntries RPC handler中当consistency check失败后要做的事情其实也就是设置`reply.ConflictIndex`和`reply.ConflictTerm`，至于怎么设置，可以根据上述leader对三种情况的处理来进行相应的设置。

    虽说这个算法只是提高了Raft算法的效率，对Raft算法的正确性没有影响，但实现这个算法是必要的，因为lab2给出的Hint：

    > **Hint**: In order to pass some of the challenging tests towards the end, such as those marked "unreliable", you will need to implement the optimization to allow a follower to back up the leader's nextIndex by more than one entry at a time. 

15. 哪些代码段需要加锁，参考[raft-locking.txt](https://pdos.csail.mit.edu/6.824/labs/raft-locking.txt)。自己写代码可能无意中就会死锁，需要通过跑测试发现然后修复。加锁其实就是创建了一片片临界区，加锁保证了，一片临界区不会插入到另一片临界区中间执行，也就是使得临界区的执行是atomic。加锁也起到同步的作用。实现时，一些代码如果没有同步，任意交叉执行，可能就产生了错误的结果，这也一样通过跑测试发现然后修复。

16. 我们需要填充Kill()，使得在本次小测试中的线程都退出，避免干扰到下一个小测试。

17. 对于测试出现的"apply error: server 2 apply out of order 249"，原因是写applyCh的代码分散在各处，且同步没做好。简单且有效的解决方法是在每一个raft server中单独开一个线程专门写applyCh，server需要响应客户端时就唤醒这个线程即可，这样可以保证按照log序写入LogEntry，不会乱序，而且无需考虑同步问题。

    


