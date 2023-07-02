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
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term int
	Idx  int
	Cmd  interface{}
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
	role        Role
	currentTerm int
	voteCount   int
	votedFor    int

	grantVoteCh chan struct{} // 用于Follower重置election timer
	leaderCh    chan struct{} // 用于Candidate 选举成功重置election timer
	heartBeatCh chan struct{}
	applych     chan ApplyMsg
	commitCh    chan struct{}
	entry       []LogEntry // 日志条目
	commitIdx   int        // 最新一次提交的日志编号
	lastApplied int        // 最新一次回应client的日志编号
	nextIdx     []int      // 送给每个服务器下一条日志条目索引号(初始化为leader的最高索引号+1)
	matchIdx    []int      // 已知要复制到每个服务器上的最高日志条目号
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
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
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	LastLogIdx  int // candidate最新日志索引号
	LastLogTerm int // LastLogIdx的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIdx   int // leader当前最大的日志索引
	PrevLogTerm  int // PrevLogIdx的任期
	Entry        []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	NextIdx int
}

func (rf *Raft) roleToString() string {
	switch rf.role {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "???"
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("%v %v receive request vote rpc from candidate %v candidate term %v cur term %v...",
		rf.roleToString(), rf.me, args.CandidateId, args.Term, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 默认不投票
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = NoOne
		rf.role = Follower
		rf.currentTerm = args.Term
	}

	update := false
	DPrintf("%v %v lastLogIdx %v lastLogTerm %v args.LastLogIdx %v args.LastLogTerm %v...",
		rf.roleToString(), rf.me, rf.lastLogIdx(), rf.lastLogTerm(), args.LastLogIdx, args.LastLogTerm)
	if args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIdx >= rf.lastLogIdx()) { // 任期号大的新
		update = true
	}

	if (rf.votedFor == NoOne || rf.votedFor == args.CandidateId) && update {
		rf.votedFor = args.CandidateId
		rf.role = Follower
		reply.VoteGranted = true
		rf.grantVoteCh <- struct{}{} // to reset election timer
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%v %v receive append entry rpc from leader %v leader's term %v cur term %v...",
		rf.roleToString(), rf.me, args.LeaderId, args.Term, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm { // 旧leader可能收到新leader的心跳消息
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.votedFor = NoOne
	}

	DPrintf("%v %v lastLogIdx %v lastLogTerm %v commitIdx %v args.PrevLogIdx %v args.PrevLogTerm %v args.LeaderCommit %v len of args.entry %v LeaderId %v...",
		rf.roleToString(), rf.me, rf.lastLogIdx(), rf.lastLogTerm(), rf.commitIdx, args.PrevLogIdx, args.PrevLogTerm, args.LeaderCommit, len(args.Entry), args.LeaderId)

	rf.heartBeatCh <- struct{}{}
	if args.PrevLogIdx < rf.commitIdx {
		return
	}

	if len(args.Entry) == 0 {
		goto COMMIT
	}

	if rf.entry[args.PrevLogIdx].Term != args.PrevLogTerm {
		// 删除冲突点以及之后的所有日志
		rf.entry = rf.entry[:args.PrevLogIdx]
		reply.NextIdx = rf.lastLogIdx() + 1
		return
	}

	rf.entry = rf.entry[:args.PrevLogIdx+1]
	rf.entry = append(rf.entry, args.Entry...)

COMMIT:
	reply.Success = true
	rf.votedFor = args.LeaderId

	if args.LeaderCommit > rf.commitIdx {
		rf.commitIdx = min(args.LeaderCommit, rf.lastLogIdx())
		rf.commitCh <- struct{}{} // 告诉主线程leader已经提交
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) HandleAppendEntryReply(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%v %v handle append entry reply from server %v...", rf.roleToString(), rf.me, id)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = NoOne
	}

	if reply.Success {
		rf.nextIdx[id] = args.PrevLogIdx + len(args.Entry) + 1
		rf.matchIdx[id] = rf.nextIdx[id] - 1
		DPrintf("server %v nextIdx %v matchIdx %v...", id, rf.nextIdx[id], rf.matchIdx[id])

		N := rf.commitIdx

		for i := N + 1; i <= rf.lastLogIdx(); i++ {
			count := 0
			for j, _ := range rf.peers {
				if j != rf.me && rf.matchIdx[j] >= i && rf.entry[i].Term == rf.currentTerm {
					count++
				}
			}

			if count > len(rf.peers)/2 {
				N = i
				break
			}
		}

		if N > rf.commitIdx {
			rf.commitIdx = N
			rf.commitCh <- struct{}{}
		}
	} else {
		rf.nextIdx[id] = reply.NextIdx
	}
}

func (rf *Raft) HandleRequestVoteReply(id int, args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("%v %v handle request vote reply from server %v...", rf.roleToString(), rf.me, id)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 已经不是candidade 或者当前任期过期了
	if rf.role != Candidate || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.role = Follower
		rf.votedFor = NoOne
		rf.voteCount = 0
		rf.currentTerm = reply.Term
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
		if rf.role == Candidate && rf.voteCount > len(rf.peers)/2 {
			rf.role = Leader
			rf.leaderCh <- struct{}{}
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.role == Leader

	if isLeader {
		DPrintf("%v %v lastLogIdx %v lastLogTerm %v append entry...", rf.roleToString(), rf.me, rf.lastLogIdx(), rf.lastLogTerm())
		rf.entry = append(rf.entry, LogEntry{Term: rf.currentTerm,
			Idx: rf.lastLogIdx() + 1,
			Cmd: command})
		index = rf.lastLogIdx()
		term = rf.lastLogTerm()
	}

	// Your code here (2B).

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

func (rf *Raft) broadCastRequestVoteRpc() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		LastLogIdx:  rf.lastLogIdx(),
		LastLogTerm: rf.lastLogTerm(),
	}
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i != rf.me && rf.role == Candidate {
			go func(id int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(id, &args, &reply)
				if ok {
					rf.HandleRequestVoteReply(id, &args, &reply)
				}
			}(i)
		}
	}
}

func (rf *Raft) broadCastAppendEntryRpc() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i != rf.me && rf.role == Leader {
			go func(id int) {
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
				}
				args.LeaderCommit = rf.commitIdx

				DPrintf("leader %v send log entry to server %v lastlogidx %v nextidx %v...", rf.me, id, rf.lastLogIdx(), rf.nextIdx[id])
				if rf.lastLogIdx() >= rf.nextIdx[id] {
					args.PrevLogIdx = rf.nextIdx[id] - 1
				} else {
					args.PrevLogIdx = rf.lastLogIdx()
				}

				args.PrevLogTerm = rf.entry[args.PrevLogIdx].Term
				args.Entry = make([]LogEntry, len(rf.entry[args.PrevLogIdx+1:]))
				copy(args.Entry, rf.entry[args.PrevLogIdx+1:])

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntry(id, &args, &reply)
				if ok {
					rf.HandleAppendEntryReply(id, &args, &reply)
				}
			}(i)
		}
	}
}

func (rf *Raft) commit() {
	for !rf.killed() {
		select {
		case <-rf.commitCh:
			rf.mu.Lock()
			for i := rf.lastApplied + 1; i <= rf.commitIdx; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					CommandIndex: i,
					Command:      rf.entry[i].Cmd,
				}
				rf.mu.Unlock()
				rf.applych <- msg
				rf.mu.Lock()
				rf.lastApplied += 1
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) run() {
	for !rf.killed() {
		switch rf.role {
		case Follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartBeatCh:
			case <-time.After(randElectionTimeOut()):
				DPrintf("server %v election timer times out Follower ---> Candidate...", rf.me)
				rf.mu.Lock()
				rf.role = Candidate
				rf.mu.Unlock()
			}
			break
		case Candidate:
			rf.mu.Lock()
			rf.currentTerm += 1
			rf.voteCount = 1
			rf.votedFor = rf.me
			rf.mu.Unlock()

			rf.broadCastRequestVoteRpc()

			select {
			case <-time.After(ElectionTimeOut):
			case <-rf.heartBeatCh: // 收到来自其他leader的心跳消息
				rf.mu.Lock()
				DPrintf("server %v is Candidate receive other leader heartBeatch Candidate ---> Follower...", rf.me)
				rf.role = Follower
				rf.mu.Unlock()
			case <-rf.leaderCh:
				DPrintf("%v %v election success, come to be Leader lastLogIdx %v lastLogTerm %v...", rf.roleToString(), rf.me, rf.lastLogIdx(), rf.lastLogTerm())
				rf.mu.Lock()
				rf.role = Leader

				// ------------ Log Replication
				rf.nextIdx = make([]int, len(rf.peers))
				rf.matchIdx = make([]int, len(rf.peers))

				DPrintf("server %v init nextIdx and matchIdx...", rf.me)
				for i, _ := range rf.peers {
					rf.nextIdx[i] = rf.lastLogIdx() + 1
					rf.matchIdx[i] = 0
				}
				rf.mu.Unlock()
			}
			break
		case Leader:
			rf.broadCastAppendEntryRpc()
			time.Sleep(HeartBeatTimeOut)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
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
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		role:        Follower,
		voteCount:   0,
		currentTerm: 0,
		votedFor:    NoOne,
		grantVoteCh: make(chan struct{}, ChanCap),
		leaderCh:    make(chan struct{}, ChanCap),
		heartBeatCh: make(chan struct{}, ChanCap),
		applych:     applyCh,
		entry:       make([]LogEntry, 0),
		commitIdx:   0,
		lastApplied: 0,
	}
	// Your initialization code here (2A, 2B, 2C).
	rf.entry = append(rf.entry, LogEntry{Idx: 0, Term: 0})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.run()
	go rf.commit()
	return rf
}
