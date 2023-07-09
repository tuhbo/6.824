package raft

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
	// NextIdx int
	XTerm int // 冲突点日志的任期
	XIdx  int // XTerm期间的第一条日志的idx
	XLen  int // follower所拥有的日志长度
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("%v %v receive append entry rpc from leader %v leader's term %v cur term %v...",
		rf.roleToString(), rf.me, args.LeaderId, args.Term, rf.currentTerm)

	reply.Term = rf.currentTerm
	reply.Success = false
	// reply.NextIdx = rf.lastLogIdx() + 1

	if args.Term < rf.currentTerm {
		DPrintf("%v %v receive old append entry ...", rf.roleToString(), rf.me)
		return
	}

	if args.Term > rf.currentTerm { // 旧leader可能收到新leader的心跳消息
		DPrintf("server %v %v ----> Follower...", rf.me, rf.roleToString())
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.votedFor = NoOne
	}

	DPrintf("%v %v lastLogIdx %v lastLogTerm %v commitIdx %v args.PrevLogIdx %v args.PrevLogTerm %v args.LeaderCommit %v len of args.entry %v LeaderId %v...",
		rf.roleToString(), rf.me, rf.lastLogIdx(), rf.lastLogTerm(), rf.commitIdx, args.PrevLogIdx, args.PrevLogTerm, args.LeaderCommit, len(args.Entry), args.LeaderId)

	rf.heartBeatCh <- struct{}{}
	rf.votedFor = args.LeaderId
	if args.PrevLogIdx < rf.commitIdx { // 已经提交的日志不能被覆盖
		reply.XTerm = Committed
		reply.XIdx = rf.commitIdx + 1
		return
	}

	if rf.lastLogIdx() < args.PrevLogIdx { // log entry的下标越界了
		reply.XTerm = OutRange
		reply.XLen = len(rf.entry)
		return
	}

	if rf.entry[args.PrevLogIdx].Term != args.PrevLogTerm {
		// /* 删除冲突点以及之后的所有日志*/
		// rf.entry = rf.entry[:args.PrevLogIdx]
		// reply.NextIdx = rf.lastLogIdx() + 1
		reply.XTerm = rf.entry[args.PrevLogIdx].Term
		reply.XIdx = args.PrevLogIdx
		for i := args.PrevLogIdx; i >= 0; i-- {
			if rf.entry[i].Term != reply.XTerm {
				reply.XIdx = i + 1
				break
			}
		}
		return
	}

	reply.Success = true
	if len(args.Entry) == 0 {
		goto COMMIT
	}

	rf.entry = rf.entry[:args.PrevLogIdx+1]
	rf.entry = append(rf.entry, args.Entry...)

COMMIT:
	if args.LeaderCommit > rf.commitIdx {
		rf.commitIdx = args.LeaderCommit
		if args.LeaderCommit > rf.lastLogIdx() {
			rf.commitIdx = rf.lastLogIdx()
		}
		DPrintf("server %v send message to commit ch commitId %v", rf.me, rf.commitIdx)
		rf.commitCh <- struct{}{} // 告诉主线程leader已经提交
	}

}

func (rf *Raft) HandleAppendEntryReply(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v %v cur term %v handle append entry reply from server %v 's term %v...", rf.roleToString(), rf.me, rf.currentTerm, id, reply.Term)

	if rf.role != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = NoOne
		rf.persist()
	}

	if reply.Success {
		rf.nextIdx[id] = args.PrevLogIdx + len(args.Entry) + 1
		rf.matchIdx[id] = rf.nextIdx[id] - 1
		DPrintf("server %v nextIdx %v matchIdx %v...", id, rf.nextIdx[id], rf.matchIdx[id])

		N := rf.commitIdx

		for i := N + 1; i <= rf.lastLogIdx(); i++ {
			count := 1
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
			DPrintf("server %v should to send message to commit ch", rf.me)
			rf.commitIdx = N
			rf.commitCh <- struct{}{}
		}
	} else {
		// rf.nextIdx[id] = reply.NextIdx
		if reply.XTerm == OutRange {
			rf.nextIdx[id] = reply.XLen
		} else if reply.XTerm == Committed {
			rf.nextIdx[id] = reply.XIdx
		} else {
			termNotExist := true

			for i := rf.lastLogIdx(); i >= 1; i-- { // 找到xterm的第一个日志条目
				if rf.entry[i].Term == reply.XTerm {
					termNotExist = false
					rf.nextIdx[id] = i
					break
				}

				if rf.entry[i].Term < reply.XTerm {
					break
				}
			}
			if termNotExist {
				rf.nextIdx[id] = reply.XIdx
			}
		}
	}
}
