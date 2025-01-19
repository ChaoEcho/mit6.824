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

type NodeState string

const (
	tickInterval               = 50 * time.Millisecond
	heartbeatTimeout           = 150 * time.Millisecond
	None                       = -1
	Follower         NodeState = "Follower"
	Candidate        NodeState = "Candidate"
	Leader           NodeState = "Leader"
)

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

	state            NodeState
	currentTerm      int
	votedFor         int
	heartbeatTimeout time.Duration
	electionTimeout  time.Duration
	lastElection     time.Time
	lastHeartbeat    time.Time
	peerTackers      []PeerTracker
}

type RequestAppendEntriesArgs struct {
	LeaderTerm   int
	PrevLogIndex int
	PrevLogTerm  int
	Logs         []ApplyMsg
	LeaderCommit int
	LeaderId     int
}

type RequestAppendEntriesReply struct {
	FollowerTerm  int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) getHeartbeatTime() time.Duration {
	return time.Millisecond * 110
}

func (rf *Raft) getElectionTime() time.Duration {
	return time.Millisecond * time.Duration(350+rand.Intn(200))
}

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
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) StartAppendEntries(heart bool) {
	args := RequestAppendEntriesArgs{}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.resetElectionTimer()
	args.LeaderTerm = rf.currentTerm
	args.LeaderId = rf.me

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.AppendEntries(i, heart, &args)
	}
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	DPrintf(111, "receiving heartbeat from leader %d and gonna get the lock...", args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true
	DPrintf(110, "\n  %d receive heartbeat at leader %d's term %d, and my term is %d", rf.me, args.LeaderId, args.LeaderTerm, rf.currentTerm)

	if args.LeaderTerm < rf.currentTerm {
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		return
	}

	rf.resetElectionTimer()

	rf.state = Follower

	if args.LeaderTerm > rf.currentTerm {
		rf.votedFor = None
		rf.currentTerm = args.LeaderTerm
		reply.FollowerTerm = rf.currentTerm
	}
}

func (rf *Raft) AppendEntries(server int, heart bool, args *RequestAppendEntriesArgs) {
	reply := RequestAppendEntriesReply{}
	rf.mu.Lock()
	DPrintf(111, "%v: is ready to send heartbeat to %d", rf.SayMeL(), server)
	rf.mu.Unlock()

	if heart {
		rf.sendRequestAppendEntries(server, args, &reply)
	}
}

func (rf *Raft) SayMeL() string {
	return "success"
}

type MyDPrintLog struct {
	Id      int
	State   NodeState
	Action  string
	Term    int
	Message string
	Extra   interface{}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// 初始化
	rf.currentTerm = 0
	rf.votedFor = None
	rf.state = Follower
	rf.heartbeatTimeout = heartbeatTimeout

	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower:
			fallthrough
		case Candidate:
			if rf.pastElectionTimeout() { //#A
				rf.StartElection()
			}
		case Leader:
			isHeartbeat := false
			if rf.pastHeartbeatTimeout() {
				isHeartbeat = true
				rf.resetHeartbeatTimer()
			}
			rf.StartAppendEntries(isHeartbeat)
		}
		time.Sleep(tickInterval)
	}
	DPrintf(110, "tim")
}
