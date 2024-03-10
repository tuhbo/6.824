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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labgob"
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

	curState       State
	curTerm        int
	voteFor        int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg
	voteCount      int
	log            []LogEntry
	commitIdx      int
	lastApplied    int
	nextIdx        []int // 待发送给每个server的下一条log idx
	matchIdx       []int // 已经被复制到每个server的最大log idx
	repCond        []*sync.Cond
	applyCond      *sync.Cond
}

func (rf *Raft) LastLogIdx() int {
	return rf.log[len(rf.log)-1].Idx
}

func (rf *Raft) LastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.curTerm
	isleader = (rf.curState == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// 持久化当前的任期，投票的id，日志
	e.Encode(rf.curTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)

	DPrintf("server[%d] term %d voteFor %d log %v", rf.me, rf.curTerm, rf.voteFor, rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurTerm int
	var VoteFor int
	var entry []LogEntry

	if d.Decode(&CurTerm) != nil || d.Decode(&VoteFor) != nil || d.Decode(&entry) != nil {
		DPrintf("read persist fail....")
		runtime.Goexit()
	} else {
		rf.curTerm = CurTerm
		rf.voteFor = VoteFor
		rf.log = entry
		DPrintf("server[%d] term %d voteFor %d log %v", rf.me, rf.curTerm, rf.voteFor, rf.log)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!

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
	isLeader := (rf.curState == Leader)

	if !isLeader {
		return index, term, false
	}

	// Your code here (2B).
	rf.mu.Lock()
	entry := LogEntry{
		Idx:  rf.LastLogIdx() + 1,
		Term: rf.curTerm,
		Cmd:  command,
	}
	rf.log = append(rf.log, entry)
	DPrintf("server[%d] start cmd %v at idx %d term %d", rf.me, command, rf.LastLogIdx(), rf.LastLogTerm())
	rf.BroadCastHeartbeat(false)
	term = rf.LastLogTerm()
	index = rf.LastLogIdx()
	rf.persist()
	rf.mu.Unlock()
	return index, term, true
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

func (rf *Raft) BroadCastHeartbeat(Heartbeat bool) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if Heartbeat {
			go rf.replicateOneRound(i)
		} else {
			rf.repCond[i].Signal()
		}
	}
	ResetTimer(rf.electionTimer, randElectionTimeOut())
	ResetTimer(rf.heartbeatTimer, HeartBeatTimeOut)
}

func (rf *Raft) replicateOneRound(id int) {
	rf.mu.Lock()
	if rf.curState != Leader {
		DPrintf("server[%d] term %d is not leader, cannot send log entry to server[%d]",
			rf.me, rf.curTerm, id)
		rf.mu.Unlock()
		return
	}
	args := AppendEntryArgs{
		Term:            rf.curTerm,
		LeaderId:        rf.me,
		LeaderCommitIdx: rf.commitIdx,
	}
	if rf.nextIdx[id] > rf.LastLogIdx() { // 不需要发送log给server id
		args.PrevLogIdx = rf.LastLogIdx()
	} else {
		args.PrevLogIdx = rf.nextIdx[id] - 1
	}
	args.PrevLogTerm = rf.log[args.PrevLogIdx].Term
	args.Entry = make([]LogEntry, len(rf.log[args.PrevLogIdx+1:]))
	copy(args.Entry, rf.log[args.PrevLogIdx+1:])
	rf.mu.Unlock()
	reply := AppendEntryReply{}

	DPrintf("server %d state %s send log to server %d lastlogidx %v prevLogIdx %d nextidx %v logs %v...",
		rf.me, stateString[rf.curState], id, rf.LastLogIdx(), args.PrevLogIdx, rf.nextIdx[id], args.Entry)
	ok := rf.sendAppendEntry(id, &args, &reply)
	if ok {
		rf.HandleAppendEntryReply(id, &args, &reply)
	}
}

func (rf *Raft) doLeaderElection() {
	args := RequestVoteArgs{
		Term:        rf.curTerm,
		CandidateId: rf.me,
		LastLogIdx:  rf.LastLogIdx(),
		LastLogTerm: rf.LastLogTerm(),
	}

	DPrintf("server[%d] start leader election at term %d", rf.me, rf.curTerm)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			reply := RequestVoteReply{}
			DPrintf("server[%d] term %d send request vote to server[%d]...", rf.me, args.Term, idx)
			ok := rf.sendRequestVote(idx, &args, &reply)
			if !ok {
				DPrintf("server[%d] term %d request vote to server[%d] failed...", rf.me, args.Term, idx)
			} else {
				rf.handleRequestVoteReply(idx, &args, &reply)
			}
		}(i)
	}
}

func (rf *Raft) ChangeState(state State) {
	DPrintf("server[%d] change %s to %s", rf.me, stateString[rf.curState], stateString[state])
	rf.curState = state
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIdx {
			rf.applyCond.Wait()
		}
		commitIdx := rf.commitIdx
		logs := make([]LogEntry, rf.commitIdx-rf.lastApplied)
		copy(logs, rf.log[rf.lastApplied+1:rf.commitIdx+1])
		rf.mu.Unlock()
		for _, entry := range logs {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Cmd,
				CommandIndex: entry.Idx,
			}
		}
		rf.mu.Lock()
		DPrintf("server[%d] apply entry %d-%d in term %d", rf.me, rf.lastApplied+1, commitIdx, rf.curTerm)
		rf.lastApplied = max(rf.lastApplied, commitIdx)
		rf.mu.Unlock()
	}
}

func (rf *Raft) tick() {
	for !rf.killed() {
		select {
		case t := <-rf.electionTimer.C:
			rf.mu.Lock()
			DPrintf("server[%d] electionTimer time out term %d State %s time %d",
				rf.me, rf.curTerm, stateString[rf.curState], t.UnixMilli())
			rf.ChangeState(Candidate)
			rf.curState = Candidate
			rf.curTerm++
			rf.voteFor = rf.me
			rf.voteCount = 1
			rf.doLeaderElection()
			rf.persist()
			ResetTimer(rf.electionTimer, randElectionTimeOut())
			rf.mu.Unlock()
		case t := <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			DPrintf("server[%d] heartbeatTimer time out term %d state %s time %d",
				rf.me, rf.curTerm, stateString[rf.curState], t.UnixMilli())
			if rf.curState == Leader {
				rf.BroadCastHeartbeat(true)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) needReplicateLog(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.curState == Leader && rf.matchIdx[peer] < rf.LastLogIdx()
}

func (rf *Raft) replicator(peer int) {
	rf.repCond[peer].L.Lock()
	for !rf.killed() {
		if !rf.needReplicateLog(peer) {
			rf.repCond[peer].Wait()
		}
		rf.replicateOneRound(peer)
	}
	rf.repCond[peer].L.Unlock()
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
		peers:          peers,
		persister:      persister,
		me:             me,
		curState:       Follower,
		voteFor:        -1,
		applyCh:        applyCh,
		electionTimer:  time.NewTimer(randElectionTimeOut()),
		heartbeatTimer: time.NewTimer(HeartBeatTimeOut),
		voteCount:      0,
		commitIdx:      0,
		lastApplied:    0,
		nextIdx:        make([]int, len(peers)),
		matchIdx:       make([]int, len(peers)),
		repCond:        make([]*sync.Cond, len(peers)),
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.log = append(rf.log, LogEntry{Idx: 0, Term: -1, Cmd: nil})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	for i := range peers {
		if i == me {
			continue
		}
		rf.matchIdx[i] = 0
		rf.nextIdx[i] = rf.LastLogIdx() + 1
		rf.repCond[i] = sync.NewCond(&sync.Mutex{})
		go rf.replicator(i)
	}
	go rf.tick()
	go rf.applier()
	return rf
}
