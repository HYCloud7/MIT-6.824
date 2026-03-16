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
	// "fmt"
	"math/rand"
	"sort"

	//"os"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func getRandMs(l, r int) int {
	return l + rand.Intn(r-l)
}

// 返回两int中的较大值（由于Go只有求两个float64的较大值的函数，且Go不支持三元运算符）
func max(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

// 返回两int中的较小值（由于Go只有求两个float64的较大值的函数，且Go不支持三元运算符）
func min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command interface{} // 客户端要求的指令
	Term    int         // 此条日志的任期号
	Index   int         // 此条日志的索引
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int // 指令执行时的term，便于kvserver的handler比较

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int           // 当前server节点投票给的节点id，初始为-1
	leaderId    int           // 当前server知道的最新的leader的id，初始为-1
	state       ServerState   // 当前server的角色状态
	timer       *time.Timer   // 计时器
	ready       bool          // 标志Candidate状态下的节点是否准备好了可以再次参与竞选
	hbTime      time.Duration // 心跳间隔（要求每秒心跳不超过十次）
	log         []LogEntry    // 此Server的日志，包含了若干日志条目，类型是日志条目的切片，第一个日志索引是1

	// 所有servers上易变的状态
	commitIndex int // 已知的已提交的日志的最大index
	lastApplied int // 应用到状态机的日志的最大index

	// leader上易变的状态（在选举后被重新初始化）
	nextIndex  []int // 对于每一个server来说，下一次要发给对应server的日志项的起始index（初始化为leader的最后一个日志条目index+1）
	matchIndex []int // 对于每一个server来说，已知成功复制到该server的最高日志项的index（初始化为0,且单调递增）

	applyCh chan ApplyMsg //  根据Make()及其他部分的注释，raft server需要维护一个发送ApplyMsg的管道

	lastIncludedIndex int // 上次快照替换的最后一个条目的index
	lastIncludedTerm  int // 上次快照替换的最后一个条目的term

	passiveSnapshotting bool // 该raft server正在进行被动快照的标志（若为true则这期间不进行主动快照）
	activeSnapshotting  bool // 该raft server正在进行主动快照的标志（若为true则这期间不进行被动快照）
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// 由kvserver调用，获取rf.passiveSnapshotting标志，若其为false，则设activeSnapshotting为true
func (rf *Raft) GetPassiveFlagAndSetActiveFlag() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.passiveSnapshotting {
		rf.activeSnapshotting = true // 若没有进行被动快照则将主动快照进行标志设为true，以便后续的主动快照检查
	}
	return rf.passiveSnapshotting
}

// 由kvserver调用修改rf.passiveSnapshotting
func (rf *Raft) SetPassiveSnapshottingFlag(flag bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.passiveSnapshotting = flag
}

// 由kvserver调用修改rf.activeSnapshotting
func (rf *Raft) SetActiveSnapshottingFlag(flag bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.activeSnapshotting = flag
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// 根据Figure2来确定应该持久化的变量，对它们编码
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)

	// raft快照中恢复时需要用到这两个，因此也持久化了
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	data := w.Bytes()
	// 调用SaveRaftState()将编码后的字节数组传递给Persister
	rf.persister.SaveRaftState(data)

}

// restore previously persisted state.
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
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry

	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("Raft server %d readPersist ERROR!\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = votedFor
		rf.log = log

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate的当前任期
	CandidatedId int // 请求投票的candidate的ID
	LastLogIndex int // candidate最后一个日志条目的index（确保安全性的选举限制用）
	LastLogTerm  int // candidate最后一个日志条目的term（确保安全性的选举限制用）
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm，用来更新candidate的term（如果需要的话）
	VoteGranted bool // 当candidate收到这张选票时为true
}

// 追加日志条目的RPC请求结构
// 心跳包是没有log entries的AppendEntries RPCs
type AppendEntriesArgs struct {
	Term         int // leader的任期
	LeaderId     int
	PreLogIndex  int        // 新条目之前的紧接着的日志的索引
	PreLogTerm   int        // 新条目之前的紧接着的日志的任期
	Entries      []LogEntry // 要存储/追加到server的日志条目，为了效率可一次追加多条（若为心跳包则此字段为空）
	LeaderCommit int        // leader提交到的日志索引位置
}

// 追加日志条目的RPC回复结构
type AppendEntriesReply struct {
	Term          int  // RPC接收server的current term，leader更新自己用（如果需要的话）
	Success       bool // 如果follower包含有匹配leader的preLogIndex以及preLogTerm的日志条目则返回true
	ConflictIndex int
	ConflictTerm  int
}

// leader向follower发送快照的RPC请求结构
type InstallSnapshotArgs struct {
	Term              int    // leader的任期
	LeaderId          int    // 便于follower将client重定向到leader
	LastIncludedIndex int    // 快照替换的最后一个条目的index
	LastIncludedTerm  int    // 快照替换的最后一个条目的term
	SnapshotData      []byte // 快照的数据
}

// leader向follower发送快照的RPC回复结构
type InstallSnapshotReply struct {
	Term   int  // RPC接收server的current term，leader更新自己用（如果需要的话）
	Accept bool // follower是否接受这个快照
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 当候选者term < 投票者的current term时直接返回false
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 当RPC请求或回复中的term > 当前server的current term时
	// 1. set currentTerm = T
	// 2. 转换到follower状态
	if args.Term > rf.currentTerm {
		// 下台并采用更高的任期，这会重新设置votedFor，就拥有新任期的投票权
		rf.voteFor = -1
		rf.leaderId = -1
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.persist()
	}

	// 判断candidate的log是否至少与这个待投票的server的log一样新，见论文5.4节，两个规则
	// 如果日志的最后条目具有不同的term，那么具有较后term的日志是更新
	// 如果日志的最后条目具有相同的term，那么哪个日志更长，哪个日志就更新
	uptodate := false
	voterLastLog := rf.log[len(rf.log)-1] // 获取投票者最后一个日志条目（如果是空日志，则获取到的是初始化时加在下标0的“占位”元素）

	// args.LastLogIndex >= voterLastLog.index需要加等号是因为第一届leader选举过程中，两个server都还没有日志
	// 它们比较的最后一个日志的term和index事实上都是初始化加入的占位元素的term和index，即term -1 == -1， index 0 == 0
	// 显然这种情况投票者是会投票给这个candidate的
	if (args.LastLogTerm > voterLastLog.Term) || (args.LastLogTerm == voterLastLog.Term && args.LastLogIndex >= voterLastLog.Index) {
		uptodate = true
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidatedId) && uptodate { // 投票给这个candidate
		rf.voteFor = args.CandidatedId
		rf.leaderId = -1 // 你投票了，说明你不信之前的leader了
		reply.VoteGranted = true
		rf.persist()
		rf.timer.Stop()
		rf.timer.Reset(time.Duration(getRandMs(300, 500)) * time.Millisecond)
	} else { // 不符合投票条件
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

// AppendEntries RPC handler
// 其他servers收到leader的追加日志rpc或心跳包后进行逻辑处理
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// leader自己的term比RPC接收server的term还要小，则追加日志失败
	// 如果AppendEntries RPC中的任期过时，则不应该重启计时器！
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 当RPC请求或回复中的term > 当前server的current term时
	// 1. set currentTerm = T
	// 2. 转换到follower状态
	// 另外，当candidate收到来自另一个声称是leader的server的RPC时，
	// 如果这个leader的term >= 这个candidate的current term，则candidate将承认这个leader是合法的，并返回到follower状态
	// wasLeader := (rf.state == Leader) // 标志rf曾经是leader

	if args.Term > rf.currentTerm {
		rf.voteFor = -1 // 当term发生变化时，需要重置votedFor
		rf.currentTerm = args.Term
		rf.persist()
	}

	// 这里实现了candidate或follower在收到leader的心跳包或日志追加RPC后重置计时器并维持follower状态
	rf.state = Follower
	rf.leaderId = args.LeaderId
	// 如果follower的term与leader的term相等（大多数情况），那么follower收到AppendEntries RPC后也需要重置计时器
	rf.timer.Stop()
	rf.timer.Reset(time.Duration(getRandMs(300, 500)) * time.Millisecond)

	// if wasLeader { // 如果是leader收到AppendEntries RPC（虽然概率很小）
	// 	go rf.ticker() // 如果是leader重回follower则要重新循环进行超时检测
	// }

	// fmt.Printf("--- server %d leaderCommitIndex is %d leader is %d --- \n", rf.me, args.LeaderCommit, rf.leaderId)

	if args.PreLogIndex < rf.lastIncludedIndex { // 如果leader发来的日志条目preLogIndex已经被follower快照替换了，则直接拒绝追加日志请求
		if len(args.Entries) == 0 || args.Entries[len(args.Entries)-1].Index < rf.lastIncludedIndex { // 如果这是一个心跳包，则说明leader的日志已经过时了，follower可以通过这个心跳包来更新leaderId和currentTerm
			reply.Term = rf.currentTerm
			reply.Success = true
			return
		} else {
			args.Entries = args.Entries[rf.lastIncludedIndex-args.PreLogIndex:]
			args.PreLogIndex = rf.lastIncludedIndex
			args.PreLogTerm = rf.lastIncludedTerm
		}
	}
	// 检查follower中是否存在leader在preLogIndex处相匹配的日志
	if rf.log[len(rf.log)-1].Index < args.PreLogIndex || rf.log[args.PreLogIndex-rf.lastIncludedIndex].Term != args.PreLogTerm {
		// 日志回溯加速优化修改
		if rf.log[len(rf.log)-1].Index < args.PreLogIndex { // 如果follower的日志中没有prevLogIndex
			reply.ConflictIndex = rf.log[len(rf.log)-1].Index + 1
			reply.ConflictTerm = -1
		} else { // 如果follower在其日志中确实有prevLogIndex，但是任期不匹配
			reply.ConflictTerm = rf.log[args.PreLogIndex-rf.lastIncludedIndex].Term
			i := args.PreLogIndex - 1 -rf.lastIncludedIndex
			for i >= 0 && rf.log[i].Term == reply.ConflictTerm { // 在其日志中搜索其条目中任期等于conflictTerm的第一个索引
				i--
			}
			reply.ConflictIndex = i + 1 + rf.lastIncludedIndex
		}

		reply.Success = false // 返回false
		reply.Term = rf.currentTerm
		return
	} else { // 匹配到了两个日志一致的最新日志条目
		// 日志一致性检查到leader让follower追加日志操作中，都用AppendEntries RPC，这样leader不用专门去恢复日志一致性
		// 即使是心跳包也无需特别处理，因为追加的日志为空，但注意心跳包也要通过一致性检查才会返回true

		// PreLogIndex与PrevLogTerm匹配到的情况，还要额外检查新同步过来的日志和已存在的日志是否存在冲突:
		// 如果一个已经存在的日志项和新的日志项冲突（相同index但是不同term），那么要删除这个冲突的日志项及其往后的日志，并将新的日志项追加到日志中。
		misMatchIndex := -1
		for i, entry := range args.Entries {
			if args.PreLogIndex+1+i > rf.log[len(rf.log)-1].Index || rf.log[args.PreLogIndex-rf.lastIncludedIndex+1+i].Term != entry.Term {
				misMatchIndex = args.PreLogIndex + 1 + i
				break
			}
		}
		if misMatchIndex != -1 { // 处理日志有冲突的情况
			newLog := rf.log[:misMatchIndex-rf.lastIncludedIndex]                                            // 从头截取到misMatchIndex（但不包括）的是一致的日志
			newLog = append(newLog, args.Entries[misMatchIndex-args.PreLogIndex-1:]...) // 追加日志中没有的任何新条目（也即leader在preLogIndex之后的日志）
			rf.log = newLog
		}

		rf.persist()

		if args.LeaderCommit > rf.commitIndex {
			// leader都还没有将所有日志提交，则follower最多提交到leader提交的位置
			// leader已经至少提交到了发给follower的最后一个日志，则follower就把自己现有的日志提交
			rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
			// fmt.Printf("--- server %d update commitIndex to %d --- \n", rf.me, rf.commitIndex)
		}
		reply.Success = true
		reply.Term = rf.currentTerm
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

// 由candidate调用此方法请求其他server投票
// 入参server是目的server在rf.peers[]中的索引（id）
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 由leader调用，向其他servers发送日志条目追加请求或心跳包（没有携带日志条目的AppendEntries RPCs）
// 入参server是目的server在rf.peers[]中的索引（id）
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply) // 调用对应server的Raft.AppendEntries方法进行请求日志追加处理
	return ok
}

// 由leader调用，向其他servers发送快照
// 入参server是目的server在rf.peers[]中的索引（id）
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply) // 调用对应server的Raft.InstallSnapshot方法安装日志
	return ok
}

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
// 接受客户端的command，并且应用raft算法
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term = rf.currentTerm
	if rf.state == Leader {
		isLeader = true
	} else {
		isLeader = false
	}

	index = rf.log[len(rf.log)-1].Index + 1 // 返回插入位置处的日志索引

	if !isLeader {
		return index, term, false
	}

	// 如果当前server是leader则开始向其他server复制日志
	newLog := LogEntry{
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.log = append(rf.log, newLog) // leader先将日志添加到自己的日志列表中
	rf.persist()
	DPrintf("[Start]Client sends a new commad(%v) to Leader %d!\n", command, rf.me)

	// 客户端发来新的command，复制日志到各server，调用LeaderAppendEntries()
	// 如果追加失败（网络问题或日志不一致被拒绝），则重复发送由携带日志条目的周期性的心跳包来完成
	go rf.LeaderAppendEntries() // 由新日志触发AppendEntries RPC的发送
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for {
		<-rf.timer.C
		if rf.killed() == true {
			return
		}

		rf.mu.Lock()
		nowState := rf.state
		rf.mu.Unlock()

		switch nowState {
		// 如果是Follower状态下定时器到时，则说明这段时间没有收到Leader的心跳或者Candidate的投票请求
		case Follower:
			rf.timer.Stop()
			rf.timer.Reset(time.Duration(getRandMs(300, 500)) * time.Millisecond)

			// 当前server参与竞选
			go rf.RunForElection()
		// 如果是Candidate状态下定时器到时，则说明是由于上一次选举出现平票导致选举失败
		case Candidate:
			rf.timer.Stop()
			rf.timer.Reset(time.Duration(getRandMs(300, 500)) * time.Millisecond)

			rf.mu.Lock()
			if rf.ready {
				rf.mu.Unlock()
				go rf.RunForElection()
			} else {
				rf.ready = true
				rf.mu.Unlock()
			}
		case Leader:
			return
		}

	}
}

// 当server节点在计时器时间内没有收到leader的心跳或者投票请求时，自己将变为Candidate
// 1.自增CurrentTerm
// 2.给自己投票
// 3.重置选举计时器
// 4.向其他所有server发送请求投票请求
func (rf *Raft) RunForElection() {
	rf.Convert2Candidate()

	rf.mu.Lock()
	sameTerm := rf.currentTerm // 记录当前的term，避免在发送RPC请求的过程中currentTerm发生变化
	rf.ready = false
	rf.mu.Unlock()

	// 使用条件变量来检测获得多数投票的条件
	votes := 1  // 给自己投一票
	finish := 1 // 已经完成投票的server数量，初始为1（自己）
	var voteMu sync.Mutex
	cond := sync.NewCond(&voteMu)

	// 向其他所有server发送请求投票请求
	for i := range rf.peers {

		// 在竞选过程中被kill掉
		if rf.killed() == true {
			return
		}

		rf.mu.Lock()
		if rf.state != Candidate { // 如果自己不再是Candidate则不继续请求投票
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		if i == rf.me {
			continue
		}

		// 开启协程并发地执行请求投票RPC
		go func(server int) {

			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         sameTerm,
				CandidatedId: rf.me,
				LastLogIndex: rf.log[len(rf.log)-1].Index,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			rf.mu.Unlock()

			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				DPrintf("Candidate %d call server %d for RequestVote failed!\n", rf.me, server)
			}

			rf.mu.Lock()
			// 处理RPC回复之前先判断，如果自己不再是Candidate了则直接返回
			// 防止任期混淆（当收到旧任期的RPC回复，比较当前任期和原始RPC中发送的任期，如果两者不同，则放弃回复并返回）
			if rf.state != Candidate || rf.currentTerm != sameTerm {
				rf.mu.Unlock()
				return
			}

			if rf.currentTerm < reply.Term {
				rf.voteFor = -1
				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.persist()
				rf.mu.Unlock()

				rf.timer.Stop()
				rf.timer.Reset(time.Duration(getRandMs(300, 500)) * time.Millisecond)
				return
			}
			rf.mu.Unlock()

			vote := reply.VoteGranted // 查看是否收到选票。如果RPC发送失败，reply中的投票仍是默认值，相当于没收到投票
			voteMu.Lock()
			if vote {
				DPrintf("Candidate %d got a vote from server %d!\n", rf.me, server)
				votes++
			}
			finish++
			voteMu.Unlock()
			cond.Broadcast() // Broadcast 会清空队列，唤醒全部的等待中的 goroutine
		}(i)
	}

	sumNum := len(rf.peers)     // 集群中总共的server数
	majorityNum := sumNum/2 + 1 // 满足大多数至少需要的server数量

	voteMu.Lock()

	// 检查是否满足“获得大多数选票”的条件
	for votes < majorityNum && finish != sumNum {
		cond.Wait() // 调用该方法的 goroutine 会被放到 Cond 的等待队列中并阻塞，直到被 Signal 或者 Broadcast 方法唤醒

		// 当candidate收到比自己term大的rpc回复时它就回到follower，此时直接结束自己的竞选
		// 或者该candidate得不到多数票但又由于有server崩溃而得不到sumNum张选票而一直等待，此时只有当有leader出现并发送心跳让该candidate变回follower跳出循环
		// 所以每次都要检测该candidate是否仍然在竞选，如果它已经退选，就不用一直等待选票了
		rf.mu.Lock()
		if rf.state != Candidate {
			rf.mu.Unlock()
			voteMu.Unlock()
			return
		}
		rf.mu.Unlock()
	}

	if votes >= majorityNum { // 满足条件，直接当选leader
		rf.Convert2Leader() // 成为leader就不需要超时计时了，直至故障或发现自己的term过时
	} else { // 收到所有回复但选票仍不够的情况，即竞选失败
		DPrintf("Candidate %d failed in the election and continued to wait...\n", rf.me)
	}

	voteMu.Unlock()

}

func (rf *Raft) Convert2Candidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Candidate
	rf.currentTerm++
	rf.leaderId = -1
	rf.voteFor = rf.me
	rf.persist()
	DPrintf("Candidate %d run for election! Its current term is %d\n", rf.me, rf.currentTerm)
}

// server转换状态成leader
// candidate收到大多数servers的投票后成功当选转为leader，candidate -> leader
func (rf *Raft) Convert2Leader() {
	rf.mu.Lock()
	DPrintf("Candidate %d was successfully elected as the leader! Its current term is %d\n", rf.me, rf.currentTerm)
	rf.state = Leader
	rf.leaderId = rf.me

	// leader上任时初始化nextIndex以及matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1 // 初始化为leader的最后一个日志条目index+1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0 // 初始化为0
	}

	rf.mu.Unlock()

	// 起一个协程循环发送心跳包，心跳间隔为100ms
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			stillLeader := (rf.state == Leader)
			rf.mu.Unlock()

			if stillLeader {
				go rf.LeaderAppendEntries()
				time.Sleep(rf.hbTime) // 心跳间隔
			} else { // 如果当前server不再是leader（变为了follower）则重启计时器并停止发送心跳包
				rf.timer.Stop()
				rf.timer.Reset(time.Duration(getRandMs(300, 500)) * time.Millisecond)
				go rf.ticker()
				return
			}
		}
	}()
}

// leader发送日志追加RPC或心跳包的相关逻辑
// leader调用LeaderAppendEntries()的两种情况：
// 1. 周期性发送心跳包
// 2. 客户端发来新的command，复制日志到各server
func (rf *Raft) LeaderAppendEntries() {
	rf.mu.Lock()
	sameTerm := rf.currentTerm // 记录rf.currentTerm的副本，在goroutine中发送RPC时使用相同的term
	rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
	rf.mu.Unlock()

	// leader向除自己以外的其他server发送AppendEntries RPC
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// 利用协程并行地发送AppendEntries RPC（包括心跳包）
		go func(idx int) {
			if rf.killed() { // 如果在发送AppendEntries RPC过程中leader被kill了就直接结束
				return
			}

			rf.mu.Lock()

			// 发送RPC之前先判断，如果自己不再是leader了则直接返回
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			appendLog := []LogEntry{} // 若为心跳包则要追加的日志条目为空切片
			nextIndex := rf.nextIndex[idx]

			if nextIndex <= rf.lastIncludedIndex { // 如果要追加的日志已经被截断了则向该follower发送快照
				go rf.LeaderSendSnapshot(idx, rf.persister.ReadSnapshot())
				rf.mu.Unlock()
				return
			}

			// 如果leader的日志从nextIdx开始有要发送的日志，则此AppendEntries RPC需要携带从nextIdx开始的日志条目
			if rf.log[len(rf.log)-1].Index >= nextIndex {
				appendLog = make([]LogEntry, len(rf.log)-nextIndex+rf.lastIncludedIndex)
				copy(appendLog, rf.log[nextIndex-rf.lastIncludedIndex:]) // 将leader日志nextIdx及之后的条目复制到appendLogs
			}
			preLog := rf.log[nextIndex-rf.lastIncludedIndex-1] // preLog是leader要发给server idx的日志条目的前一个日志条目

			args := AppendEntriesArgs{
				Term:         sameTerm,
				LeaderId:     rf.me,
				PreLogIndex:  preLog.Index,
				PreLogTerm:   preLog.Term,
				Entries:      appendLog,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}

			DPrintf("Leader %d sends AppendEntries RPC(term:%d, Entries len:%d, PreLogIndex:%v,leaderCommit:%v) to server %d...\n",
				rf.me, sameTerm, len(args.Entries), args.PreLogIndex, args.LeaderCommit, idx)

			ok := rf.sendAppendEntries(idx, &args, &reply) // leader向 server i 发送AppendEntries RPC

			if !ok {
				// 如果由于网络原因或者follower故障等收不到RPC回复（不是follower将回复设为false）
				// 则leader无限期重复发送同样的RPC（nextIndex不前移），等到下次心跳时间到了后再发送
				DPrintf("Leader %d calls server %d for AppendEntries or Heartbeat failed!\n", rf.me, idx)
				return
			}

			// 如果leader收到比自己任期更大的server的回复，则leader更新自己的任期并转为follower，跟随此server
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 处理RPC回复之前先判断，如果自己不再是leader了则直接返回
			// 防止任期混淆（当收到旧任期的RPC回复，比较当前任期和原始RPC中发送的任期，如果两者不同，则放弃回复并返回）
			if rf.state != Leader || rf.currentTerm != args.Term {
				return
			}

			if rf.currentTerm < reply.Term {
				rf.voteFor = -1
				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.persist()
				return
			}

			// 如果出现follower的日志与leader的不一致，即append失败
			if reply.Success == false { // follower拒绝接受日志的情况（不一致）
				possibleNextIdx := 0 // 可能的nextIndex[idx]

				if reply.ConflictTerm == -1 { // 如果follower的日志中没有prevLogIndex
					possibleNextIdx = reply.ConflictIndex // 这里需要提前判断节省时间，否则后面2C部分测试会FAIL
				} else {
					foundConflictTerm := false

					// 从后往前找
					k := len(rf.log) - 1
					for ; k > 0; k-- {
						if rf.log[k].Term == reply.ConflictTerm {
							foundConflictTerm = true
							break
						}
					}

					if foundConflictTerm {
						possibleNextIdx = rf.log[k+1].Index // 若找到了对应的term，则找到对应term出现的最后一个日志条目的下一个日志条目
					} else {
						possibleNextIdx = reply.ConflictIndex
					}

				}
				// fmt.Printf("--- server %d possibleNextIndex is %d and nextIndex is %d matchIndex is %d ---\n", idx, possibleNextIdx, rf.nextIndex[idx],rf.matchIndex[idx])
				if possibleNextIdx < rf.nextIndex[idx] && possibleNextIdx > rf.matchIndex[idx] {
					rf.nextIndex[idx] = possibleNextIdx
				} else { // 若不满足则视为过时，舍弃掉这次RPC回复
					return
				}

			} else { // 追加成功
				// 更新对应follower的nextIndex和matchIndex

				// 根据guide，你不能假设server的状态在它发送RPC和收到回复之间没有变化。
				// 因为可能在这期间收到新的指令而改变了log和nextIndex
				// 通常 nextIndex = matchIndex + 1
				possibleMatchIdx := args.PreLogIndex + len(args.Entries)       // 可能的matchIndex[idx]
				rf.matchIndex[idx] = max(possibleMatchIdx, rf.matchIndex[idx]) // 保证matchIndex单调递增，因为不可靠网络下会出现RPC延迟
				rf.nextIndex[idx] = rf.matchIndex[idx] + 1                     // matchIndex安全则nextIndex这样也安全

				// 更新leader的commitIndex
				// leader提交日志的条件是：存在一个N，使得N > commitIndex，并且大多数servers（包括leader自己）都已经复制了日志条目N，并且这个日志条目N是leader当前任期的日志条目
				sortMatchIndex := make([]int, len(rf.peers))
				copy(sortMatchIndex, rf.matchIndex)
				sort.Ints(sortMatchIndex) // 将matchIndex排序，满足大多数条件的N就是排序后第len(rf.peers)/2个元素

				maxN := sortMatchIndex[(len(sortMatchIndex)-1)/2] // 满足大多数条件的N
				for N := maxN; N > rf.commitIndex; N-- {
					if rf.log[N-rf.lastIncludedIndex].Term == rf.currentTerm { // 满足日志条目N是leader当前任期的日志条目，这一步保证了新leader虽然可以把老的日志复制给follower但是，新leader只会提交自己任期内的日志
						rf.commitIndex = N // 如果log[N]的任期等于当前任期则更新commitIndex
						DPrintf("Leader%d's commitIndex is updated to %d.\n", rf.me, N)
						break
					}
				}
				return
			}

		}(i)
	}
}

// leader发送SnapShot信息给落后的Follower
// idx是要发送给的follower的序号
func (rf *Raft) LeaderSendSnapshot(idx int, snapshot []byte) {
	rf.mu.Lock()
	sameTerm := rf.currentTerm // 记录rf.currentTerm的副本，在goroutine中发送RPC时使用相同的term
	rf.mu.Unlock()

	// leader像follower[idx]发送InstallSnapshot RPC

	if rf.killed() { // 如果在发送InstallSnapshot RPC过程中leader被kill了就直接结束
		return
	}

	rf.mu.Lock()

	// 发送RPC之前先判断，如果自己不再是leader了则直接返回
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term:              sameTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		SnapshotData:      snapshot,
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}

	DPrintf("Leader %d sends InstallSnapshot RPC(term:%d, lastIncludedIndex:%d, lastIncludedTerm:%d) to server %d...\n",
		rf.me, sameTerm, args.LastIncludedIndex, args.LastIncludedTerm, idx)

	ok := rf.sendInstallSnapshot(idx, &args, &reply) // leader向 server idx 发送InstallSnapshot RPC

	if !ok {
		DPrintf("Leader %d calls server %d for InstallSnapshot(term:%d, LastIncludedIndex:%d, LastIncludedTerm:%d) failed!\n",
			rf.me, idx, sameTerm, args.LastIncludedIndex, args.LastIncludedTerm)
		// 如果由于网络原因或者follower故障等收不到RPC回复
		return
	}

	// 如果leader收到比自己任期更大的server的回复，则leader更新自己的任期并转为follower，跟随此server
	rf.mu.Lock() //要整体加锁，不能只给if加锁然后解锁
	defer rf.mu.Unlock()

	// 处理RPC回复之前先判断，如果自己不再是leader了则直接返回
	// 防止任期混淆（当收到旧任期的RPC回复，比较当前任期和原始RPC中发送的任期，如果两者不同，则放弃回复并返回）
	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	if rf.currentTerm < reply.Term {
		rf.voteFor = -1             // 当term发生变化时，需要重置votedFor
		rf.state = Follower         // 变回Follower
		rf.currentTerm = reply.Term // 更新自己的term为较新的值
		rf.persist()
		return // 这里只是退出了协程
	}

	// 若follower接受了leader的快照则leader需要更新对应的matchIndex和nextIndex等（也得保证递增性，不能回退）
	if reply.Accept {
		possibleMatchIdx := args.LastIncludedIndex
		rf.matchIndex[idx] = max(possibleMatchIdx, rf.matchIndex[idx]) // 保证matchIndex单调递增，因为不可靠网络下会出现RPC延迟
		rf.nextIndex[idx] = rf.matchIndex[idx] + 1                     // matchIndex安全则nextIndex这样也安全
	}
}

// 被动快照
// follower接收leader发来的InstallSnapshot RPC的handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Figure 13 rules[1]
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Accept = false
		return
	}

	// 接受leader的被动快照前先检查给server是否正在进行主动快照，若是则本次被动快照取消
	// 避免主、被动快照重叠应用导致上层kvserver状态与下层raft日志不一致
	if rf.activeSnapshotting {
		reply.Term = rf.currentTerm
		reply.Accept = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.voteFor = -1            // 当term发生变化时，需要重置votedFor
		rf.currentTerm = args.Term // 更新自己的term为较新的值
		rf.persist()
	}

	rf.state = Follower         // 变回或维持Follower
	rf.leaderId = args.LeaderId // 将rpc携带的leaderId设为自己的leaderId，记录最近的leader（client寻找leader失败时用到）

	// 如果follower的term与leader的term相等（大多数情况），那么follower收到InstallSnapshot RPC后也需要重置计时器
	rf.timer.Stop()
	rf.timer.Reset(time.Duration(getRandMs(300, 500)) * time.Millisecond)

	snapshotIndex := args.LastIncludedIndex
	snapshotTerm := args.LastIncludedTerm
	reply.Term = rf.currentTerm

	if snapshotIndex <= rf.lastIncludedIndex { // 说明snapshotIndex之前的log已经做成snapshot并删除了
		DPrintf("Server %d refuse the snapshot from leader.\n", rf.me)
		reply.Accept = false
		return
	}

	// 如果leader传来的快照比本地的快照更新
	rf.lastApplied = args.LastIncludedIndex // 下一条指令直接从快照后开始（重新）apply

	var newLog []LogEntry
	if snapshotIndex < rf.log[len(rf.log)-1].Index { // 若应用此次快照，本地还有日志要接上
		// 若快照和本地日志在snapshotIndex索引处的日志term不一致，则扔掉本地快照后的所有日志
		if rf.log[snapshotIndex-rf.lastIncludedIndex].Term != snapshotTerm {
			newLog = []LogEntry{{Term: snapshotTerm, Index: snapshotIndex}}
			rf.commitIndex = args.LastIncludedIndex // 由于后面被截断的日志无效，故等重新计算commitIndex
		} else { // 若term没冲突，则在snapshotIndex处截断并保留后续日志
			newLog = []LogEntry{{Term: snapshotTerm, Index: snapshotIndex}}
			newLog = append(newLog, rf.log[snapshotIndex-rf.lastIncludedIndex+1:]...)
			rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex) // 后面日志有效则保证commit不回退
		}
	} else { // 若此快照比本地日志还要长，则应用后日志就清空了
		newLog = []LogEntry{{Term: snapshotTerm, Index: snapshotIndex}}
		rf.commitIndex = args.LastIncludedIndex
	}

	// 更新相关变量
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.log = newLog
	rf.passiveSnapshotting = true

	rf.persist() // 持久化状态
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.SnapshotData)

	// 向kvserver发送带快照的ApplyMsg通知它安装
	rf.InstallSnapFromLeader(snapshotTerm, snapshotIndex, args.SnapshotData)

	DPrintf("Server %d accept the snapshot from leader(lastIncludedIndex=%v, lastIncludedTerm=%v).\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
	reply.Accept = true
}

// 循环检查是否有需要apply的日志
func (rf *Raft) applier() {
	for !rf.killed() { // 如果server没有被kill就一直检测
		rf.mu.Lock()

		var applyMsg ApplyMsg
		needApply := false

		// 如果commitIndex > lastApplied，说明有新的日志需要apply到状态机了
		if rf.commitIndex > rf.lastApplied {

			rf.lastApplied++

			if rf.lastApplied <= rf.lastIncludedIndex { // 说明这条命令已经被做成snapshot了，不需要提交
				rf.lastApplied = rf.lastIncludedIndex // 直接将lastApplied提前到lastIncludedIndex
				rf.mu.Unlock()                        // continue前不要忘了先解锁！！！
				continue
			}

			applyMsg = ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex].Command, // 需要apply的日志条目的command
				CommandIndex: rf.lastApplied,                                      // 需要apply的日志条目的index
				CommandTerm:  rf.log[rf.lastApplied-rf.lastIncludedIndex].Term,    // 需要apply的日志条目的term

				SnapshotValid: false,
			}

			needApply = true
		}
		rf.mu.Unlock()

		if needApply {
			// rf.mu.Lock()
			// fmt.Printf("Server %d applies a new log entry! Its index is %d, command is %v, the leader is %d.\n", rf.me, applyMsg.CommandIndex, applyMsg.Command, rf.leaderId)
			// rf.mu.Unlock()
			rf.applyCh <- applyMsg // 将需要apply的日志条目的command通过applyCh发送给服务或测试者
		} else {
			time.Sleep(10 * time.Millisecond) // 不要让循环一直连续执行，可能占用很多时间而测试失败
		}

	}
}

// 如果接受，则follower将leader发来的快照发到applyCh便于状态机安装
func (rf *Raft) InstallSnapFromLeader(snapshotTerm int, snapshotIndex int, snapshotData []byte) {
	// follower接收到leader发来的InstallSnapshot RPC后先不要安装快照，而是发给状态机，判断为较新的快照时raft层才进行快照
	snapshotMsg := ApplyMsg{
		SnapshotValid: true,
		CommandValid:  false,
		SnapshotIndex: snapshotIndex,
		SnapshotTerm:  snapshotTerm,
		Snapshot:      snapshotData,
	}

	rf.applyCh <- snapshotMsg // 将包含快照的ApplyMsg发送到applyCh，等待状态机处理
	DPrintf("Server %d send SnapshotMsg(snapIndex=%v) to ApplyCh.\n", rf.me, snapshotIndex)

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

// raft层接收对应kvserver传来的快照将包含的最后一个index以及快照数据并进行快照
// leader or follower都可以主动快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()

	// 如果主动快照的index不大于rf之前的lastIncludedIndex（这次快照其实是重复或更旧的），则不应用该快照
	if index <= rf.lastIncludedIndex {
		DPrintf("Server %d refuse this positive snapshot(index=%v, rf.lastIncludedIndex=%v).\n", rf.me, index, rf.lastIncludedIndex)
		rf.mu.Unlock()
		return
	}

	DPrintf("Server %d start to positively snapshot(rf.lastIncluded=%v, snapshotIndex=%v).\n", rf.me, rf.lastIncludedIndex, index)

	// 修剪log[]，将index及以前的日志条目剪掉
	var newLog = []LogEntry{{Term: rf.log[index-rf.lastIncludedIndex].Term, Index: index}} // 裁剪后依然log索引0处用一个占位entry，不实际使用
	newLog = append(newLog, rf.log[index-rf.lastIncludedIndex+1:]...)                      // 这样可以避免原log底层数组由于有部分在被引用而无法将剪掉的部分GC（真正释放）
	rf.log = newLog
	rf.lastIncludedIndex = newLog[0].Index
	rf.lastIncludedTerm = newLog[0].Term
	// 主动快照时lastApplied、commitIndex一定在snapshotIndex之后，因此不用更新

	// 通过persister进行持久化存储
	rf.persist() // 先持久化raft state（因为rf.log，rf.lastIncludedIndex，rf.lastIncludedTerm改变了）
	state := rf.persister.ReadRaftState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)

	isLeader := (rf.state == Leader)
	rf.mu.Unlock()

	// leader通过InstallSnapshot RPC将本次的SnapShot信息发送给其他Follower
	if isLeader {
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.LeaderSendSnapshot(i, snapshot)
		}
	}
}

func (rf *Raft) recoverFromSnap(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // 如果没有快照则直接返回
		return
	}

	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex

	// raft恢复后向kvserver发送快照
	snapshotMsg := ApplyMsg{
		SnapshotValid: true,
		CommandValid:  false,
		SnapshotIndex: rf.lastIncludedIndex,
		SnapshotTerm:  rf.lastIncludedTerm,
		Snapshot:      snapshot, // sm_state
	}

	go func(msg ApplyMsg) {
		rf.applyCh <- msg // 将包含快照的ApplyMsg发送到applyCh，等待状态机处理
	}(snapshotMsg)

	DPrintf("Server %d recover from crash and send SnapshotMsg to ApplyCh.\n", rf.me)

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0 // 0代表该server还存活，1代表它被kill了

	// Your initialization code here (2A, 2B, 2C).
	// var mutex sync.Mutex
	// rf.mu = mutex
	rf.currentTerm = 0
	rf.leaderId = -1
	rf.voteFor = -1
	rf.state = Follower
	rf.ready = false
	// raft server节点每300-500ms尝试参与竞选
	rf.timer = time.NewTimer(time.Duration(getRandMs(300, 500)) * time.Millisecond)
	rf.hbTime = 100 * time.Millisecond // 心跳间隔设为100ms一次

	// 一开始没有日志条目
	// 由于合法日志索引从1开始，为了让日志index与切片下标对应，故先填充一个元素
	// 这个元素不是日志，term值和index值非法，在leader选举过程中也免去了单独讨论空日志的情况，不用怕空指针报错
	// 注意，这里占位日志的index应设为0而非其他值！！！
	rf.log = []LogEntry{{Term: -1, Index: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1

	rf.passiveSnapshotting = false
	rf.activeSnapshotting = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.recoverFromSnap(persister.ReadSnapshot()) // 从快照中恢复
	rf.persist()

	// start ticker goroutine to start elections
	go rf.ticker()

	// 起一个goroutine循环检查是否有需要应用到状态机日志
	go rf.applier()

	return rf
}
