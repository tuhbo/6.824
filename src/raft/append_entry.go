package raft

type AppendEntryArgs struct {
	Term     int
	LeaderId int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) HandleAppendEntryReply(idx int, args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("server[%d] term %d state %s receive server[%d] append entry reply args'term %d reply'term %d res %t",
		rf.me, rf.curTerm, stateString[rf.curState], idx, args.Term, reply.Term, reply.Success)
	if rf.curState != Leader || args.Term != rf.curTerm {
		DPrintf("server[%d] receive stale append entry reply rpc from server[%d]")
		return
	}
	if reply.Term > rf.curTerm {
		rf.curTerm = reply.Term
		rf.ChangeState(Follower)
		rf.voteFor = -1
		return
	}
	if reply.Success {
		ResetTimer(rf.electionTimer, randElectionTimeOut())
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.curTerm
	DPrintf("server[%d] curTerm %d state %s receive server[%d] term %d append entry",
		rf.me, term, stateString[rf.curState], args.LeaderId, args.Term)
	reply.Term = term
	reply.Success = false
	if args.Term < term {
		DPrintf("server[%d] receive stale server[%d] append entry", rf.me, args.LeaderId)
		return
	}
	if args.Term > term {
		DPrintf("server[%d]'s term %d expire, set server[%d] term %d", rf.me, rf.curTerm, args.LeaderId, args.Term)
		rf.curTerm = args.Term
		rf.voteFor = -1
		rf.ChangeState(Follower)
	}
	ResetTimer(rf.electionTimer, randElectionTimeOut())
	reply.Success = true
	reply.Term = rf.curTerm
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}
