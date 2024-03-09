package raft

type AppendEntryArgs struct {
	Term            int
	LeaderId        int
	PrevLogIdx      int // 待发送的Entry[0].idx - 1
	PrevLogTerm     int
	Entry           []LogEntry
	LeaderCommitIdx int
}

type AppendEntryReply struct {
	Term         int
	Success      bool
	ConflictTerm int
	ConflictIdx  int
}

func (rf *Raft) HandleAppendEntryReply(id int, args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("server[%d] term %d state %s receive server[%d] append entry reply args'term %d reply'term %d res %t",
		rf.me, rf.curTerm, stateString[rf.curState], id, args.Term, reply.Term, reply.Success)
	if rf.curState != Leader || args.Term != rf.curTerm {
		DPrintf("server[%d] receive stale append entry reply rpc from server[%d]")
		return
	}
	if reply.Term > rf.curTerm {
		rf.curTerm = reply.Term
		rf.ChangeState(Follower)
		rf.voteFor = -1
		rf.persist()
		return
	}
	if reply.Success {
		rf.nextIdx[id] = args.PrevLogIdx + len(args.Entry) + 1
		rf.matchIdx[id] = args.PrevLogIdx + len(args.Entry)
		DPrintf("server[%d]  server %d'nextIdx %v matchIdx %v...", rf.me, id, rf.nextIdx[id], rf.matchIdx[id])

		N := rf.commitIdx
		for i := N + 1; i <= rf.LastLogIdx(); i++ {
			cnt := 1
			for id := range rf.peers {
				if id != rf.me && rf.matchIdx[id] >= i && rf.log[i].Term == rf.curTerm {
					cnt++
				}
			}
			if cnt > len(rf.peers)/2 {
				N = i
				break
			}
		}
		if N != rf.commitIdx {
			rf.commitIdx = N
			DPrintf("server[%d] term %d commitidx %d should commit", rf.me, rf.curTerm, rf.commitIdx)
			rf.applyCond.Signal()
		}
	} else {
		DPrintf("server[%d] receive server[%d] term conflict reply.ConflictTerm %d reply.ConflictIdx %d",
			rf.me, id, reply.ConflictTerm, reply.ConflictIdx)
		if reply.ConflictTerm == IdxOutRange || reply.ConflictTerm == IdxCommited {
			rf.nextIdx[id] = reply.ConflictIdx
		} else {
			termNotExist := true
			for i := rf.LastLogIdx(); i >= 1; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					termNotExist = false
					rf.nextIdx[id] = i + 1
					break
				}

				// i之前的term肯定比reply.ConflictTerm都小
				if rf.log[i].Term < reply.ConflictTerm {
					break
				}
			}

			if termNotExist {
				rf.nextIdx[id] = reply.ConflictIdx
			}
		}
	}
}

func (rf *Raft) getConflictIdxAndTerm(PrevLogIdx int) (int, int) {
	conflictTerm := rf.log[PrevLogIdx].Term
	conflictIdx := 0
	for i := PrevLogIdx; i >= 0; i-- {
		if rf.log[i].Term != conflictTerm || i == 0 {
			conflictIdx = i + 1
			break
		}
	}
	return conflictIdx, conflictTerm
}

func (rf *Raft) AdvanceCommitIdx(LeaderCommitIdx int) {
	if LeaderCommitIdx > rf.commitIdx {
		rf.commitIdx = LeaderCommitIdx
		if rf.commitIdx > rf.LastLogIdx() {
			rf.commitIdx = rf.LastLogIdx()
		}
		rf.applyCond.Signal()
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.curTerm
	DPrintf("server[%d] curTerm %d state %s receive server[%d] term %d append entry %v"+
		"leadercommit %d PrevLogIdx %d PrevLogTerm %d lastlogidx %d lastlogterm %d commitidx %d",
		rf.me, term, stateString[rf.curState], args.LeaderId, args.Term, args.Entry, args.LeaderCommitIdx,
		args.PrevLogIdx, args.PrevLogTerm, rf.LastLogIdx(), rf.LastLogTerm(), rf.commitIdx)
	reply.Term = term
	reply.Success = false
	var needPersist = false
	if args.Term < term {
		DPrintf("server[%d] receive stale server[%d] append entry", rf.me, args.LeaderId)
		return
	}
	if args.Term > term {
		DPrintf("server[%d]'s term %d expire, set server[%d] term %d", rf.me, rf.curTerm, args.LeaderId, args.Term)
		rf.curTerm = args.Term
		rf.voteFor = -1
		rf.ChangeState(Follower)
		needPersist = true
	}
	ResetTimer(rf.electionTimer, randElectionTimeOut())

	if args.PrevLogIdx > rf.LastLogIdx() {
		DPrintf("server[%d] state %s receive server[%d] prevlogidx %d out of range",
			rf.me, stateString[rf.curState], rf.me, args.PrevLogIdx)
		reply.Success = false
		reply.Term = rf.curTerm
		reply.ConflictIdx = len(rf.log)
		reply.ConflictTerm = IdxOutRange
		if needPersist {
			rf.persist()
		}
		return
	}

	if args.PrevLogIdx < rf.commitIdx {
		DPrintf("server[%d] state %s receive server[%d] prevlogidx %d cann't cover commitidx %d",
			rf.me, stateString[rf.curState], args.PrevLogIdx, rf.commitIdx)
		reply.Success = false
		reply.Term = rf.curTerm
		reply.ConflictIdx = rf.commitIdx + 1
		reply.ConflictTerm = IdxCommited
		if needPersist {
			rf.persist()
		}
		return
	}

	if rf.log[args.PrevLogIdx].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.curTerm
		// 往前查找与冲突点term相同的第一条日志的idx
		reply.ConflictIdx, reply.ConflictTerm = rf.getConflictIdxAndTerm(args.PrevLogIdx)
		DPrintf("server[%d] conflictIdx %d conflictterm %d", reply.ConflictIdx, reply.ConflictTerm)
		if needPersist {
			rf.persist()
		}
		return
	}

	rf.log = rf.log[:args.PrevLogIdx+1]
	rf.log = append(rf.log, args.Entry...)
	DPrintf("server[%d] logs %v", rf.me, rf.log)
	reply.Success = true
	reply.Term = rf.curTerm
	rf.AdvanceCommitIdx(args.LeaderCommitIdx)
	rf.persist()
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}