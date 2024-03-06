package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) handleRequestVoteReply(idx int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.curTerm
	DPrintf("server[%d] recieve server[%d] request "+
		"vote reply curTerm %d argsTerm %d replyTerm %d voted %t", rf.me, idx, term,
		args.Term, reply.Term, reply.VoteGranted)
	if rf.curState != Candidate || args.Term != term {
		DPrintf("server[%d] receive stale request vote reply rpc from server[%d]", rf.me, idx)
		return
	}

	if reply.Term > term { // 收到任期比自己高的server回包
		rf.curTerm = reply.Term
		rf.curState = Follower
		rf.voteFor = -1
		rf.voteCount = 0
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
		if rf.curState == Candidate && rf.voteCount > len(rf.peers)/2 {
			DPrintf("server[%d] become leader at term %d", rf.me, rf.curTerm)
			rf.ChangeState(Leader)
			rf.BroadCastHeartbeat(true)
		}
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server[%d] receive server[%d] term %d request vote at term %d state %s",
		rf.me, args.CandidateId, args.Term, rf.curTerm, stateString[rf.curState])
	reply.Term = rf.curTerm
	reply.VoteGranted = false

	// 过期的投票请求
	if args.Term < rf.curTerm {
		return
	}

	// 只有一票，但已经给别人投票了
	if args.Term == rf.curTerm && (rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		return
	}

	if args.Term > rf.curTerm {
		rf.ChangeState(Follower)
		rf.curTerm = args.Term
		rf.voteFor = -1
	}

	rf.voteFor = args.CandidateId
	ResetTimer(rf.electionTimer, randElectionTimeOut())
	reply.Term = rf.curTerm
	reply.VoteGranted = true
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
