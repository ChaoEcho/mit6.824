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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
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

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器上的持久性状态
	// 当前任期
	currentTerm int
	// 投票给的候选者
	votedFor int
	// 日志
	logs []LogEntry

	// 所有服务器上的非持久性状态
	// 已提交的日志索引
	commitIndex int
	// 已应用的日志索引
	lastApplied int

	// Leader的非持久性状态
	// 已发送的日志索引
	nextIndex []int
	// 已确认的日志索引
	matchIndex []int

	// 个人添加属性
	// 节点状态
	state NodeState
	// 选举超时时间
	electionTimeout int
	// 投票chan
	voteChan chan interface{}
	// 心跳chan
	appendChan chan interface{}
	// 票数
	voteCount int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type NodeState string

const (
	Follower  NodeState = "Follower"
	Candidate NodeState = "Candidate"
	Leader    NodeState = "Leader"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	// TODO: 需要加锁
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	// 候选者的任期
	Term int
	// 候选者的ID
	CandidateId int
	// 候选者的最后日志索引
	LastLogIndex int
	// 候选者的最后日志任期
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 确保只有在需要时才发送信号
	// if args.Term >= rf.currentTerm {
	// 	rf.appendChan <- interface{}(true)
	// }

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		if reply.VoteGranted {
			DPrintf("I am %d, I got a vote from %d", rf.me, server)
			rf.mu.Lock()
			// 获得投票
			if rf.state == Candidate {
				rf.voteChan <- interface{}(true)
				rf.voteCount++
			}
			rf.mu.Unlock()
		}
	}
	return ok
}

func (rf *Raft) sendRequestVoteToAll() {
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
			// TODO: 这里需要修改，lab 3A不包含任何日志
			LastLogIndex: 0,
			LastLogTerm:  0,
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(i, args, reply)
	}
}

type AppendEntriesArgs struct {
	// 领导者的任期
	Term int
	// 领导者的ID
	LeaderId int
	// 领导者的最后日志索引
	PrevLogIndex int
	// 领导者的最后日志任期
	PrevLogTerm int
	// 领导者的日志
	Entries []LogEntry
	// 领导者的已提交的日志索引
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// TODO: 具体追加日志逻辑有点商榷
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Candidate {
		// DPrintf("I am %d,I am a candidate,I am a follower now", rf.me)
		rf.becomeFollower()
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.appendChan <- interface{}(true)
	rf.currentTerm = args.Term

	//TODO: 日志处理逻辑暂时不实现

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if reply.Success {
			// rf.mu.Lock()
			// TODO：3A不需要修改
			// rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			// 如果返回的任期大于当前任期，则更新当前任期，并转换为跟随者
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.becomeFollower()
				rf.votedFor = args.LeaderId
			}
			rf.mu.Unlock()
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntriesToAll() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			// TODO: 这里需要修改，lab 3A不包含任何日志
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			LeaderCommit: 0,
		}
		reply := &AppendEntriesReply{}
		go rf.sendAppendEntries(i, args, reply)
	}
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

const (
	heartbeatInterval  = 150
	electionTimeoutMin = 1000
	electionTimeoutMax = 1500
)

func (rf *Raft) ticker() {
	for rf.killed() == false {
		switch rf.state {
		case Follower:
			select {
			case <-rf.appendChan:
				//DPrintf("I am %d,I am a follower,I receive a heartbeat", rf.me)
			case <-time.After(time.Duration(rf.electionTimeout) * time.Millisecond):
				rf.mu.Lock()
				rf.becomeCandidate()
				rf.mu.Unlock()
			}
		case Candidate:
			go rf.sendRequestVoteToAll()
			select {
			case <-rf.voteChan:
				if rf.voteCount > len(rf.peers)/2 {
					rf.mu.Lock()
					rf.becomeLeader()
					rf.mu.Unlock()
				}
			case <-time.After(time.Duration(rf.electionTimeout) * time.Millisecond):
				rf.mu.Lock()
				rf.becomeCandidate()
				rf.mu.Unlock()
			}
		case Leader:
			rf.sendAppendEntriesToAll()
			time.Sleep(time.Duration(heartbeatInterval) * time.Millisecond)
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.currentTerm++
	DPrintf("I am %d,I am a leader,my term is %d", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	//rf.votedFor = rf.me
	//rf.voteCount = 1
	DPrintf("I am %d,I am a candidate,my term is %d", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeFollower() {
	rf.state = Follower
	DPrintf("I am %d,I am a follower,my term is %d", rf.me, rf.currentTerm)
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

	// Your initialization code here (3A, 3B, 3C).

	// 初始化
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.logs = []LogEntry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = Follower
	rf.electionTimeout = electionTimeoutMin + rand.Intn(electionTimeoutMax-electionTimeoutMin) + rand.Intn(5)*heartbeatInterval
	rf.voteChan = make(chan interface{})
	rf.appendChan = make(chan interface{})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("I am %d, I am a %v,my term is %d,my election timeout is %d", rf.me, rf.state, rf.currentTerm, rf.electionTimeout)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
