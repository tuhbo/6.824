package raft

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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("%v %v receive request vote rpc from candidate %v candidate term %v cur term %v...",
		rf.roleToString(), rf.me, args.CandidateId, args.Term, rf.currentTerm)
	defer rf.mu.Unlock()
	defer rf.persist()

	// 默认不投票
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		DPrintf("%v %v receive old request vote ...", rf.roleToString(), rf.me)
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
		rf.currentTerm = reply.Term
		rf.persist()
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
