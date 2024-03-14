package raft

import (
	"bytes"

	"6.824/src/labgob"
)

type InstallSnapShotArgs struct {
	Term             int
	LeaderId         int
	LastIncludedIdx  int
	LastIncludedTerm int
	Snapshot         []byte
}

type InstallSnapShotReply struct {
	Term int
}

func (rf *Raft) truncateLog(lastIncludedIdx int, lastIncludedTerm int) {
	DPrintf("server[%d] lastIncludedIdx %d lastIncludedTerm %d truncate log... old log size..%v, entries..%v",
		rf.me, lastIncludedIdx, lastIncludedTerm, len(rf.log), rf.log)
	idx := 0

	// 从后往前找到lastIncludedIdx, lastIncludedTerm的log
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Idx == lastIncludedIdx &&
			rf.log[i].Term == lastIncludedTerm {
			idx = i
			break
		}
	}

	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Term: lastIncludedTerm, Idx: lastIncludedIdx})
	if idx != 0 {
		for i := idx + 1; i < len(rf.log); i++ {
			newLog = append(newLog, rf.log[i])
		}
	}
	rf.log = newLog
	DPrintf("server[%d] lastIncludedIdx %d lastIncludedTerm %d truncate log... old log size..%v, entries..%v",
		rf.me, lastIncludedIdx, lastIncludedTerm, len(rf.log), rf.log)
}

func (rf *Raft) StateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	DPrintf("server[%d] curTerm %d state %s receive server[%d] term %d install snapshot args %v",
		rf.me, rf.curTerm, args.LeaderId, args.Term, *args)
	reply.Term = rf.curTerm
	if args.Term < rf.curTerm {
		DPrintf("server[%d] state %s term %d receive server[%d] stale term %d snapshot...",
			rf.me, stateString[rf.curState], rf.curTerm, args.LeaderId, args.Term)
		rf.mu.Unlock()
		return
	}
	var needPersist bool
	if args.Term > rf.curTerm {
		rf.ChangeState(Follower)
		rf.curTerm = args.Term
		rf.voteFor = -1
		needPersist = true
	}
	reply.Term = rf.curTerm
	rf.voteFor = args.LeaderId
	ResetTimer(rf.electionTimer, randElectionTimeOut())
	if args.LastIncludedIdx <= rf.commitIdx {
		DPrintf("server [%d] rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger",
			rf.me, args.LastIncludedIdx, rf.commitIdx)
		if needPersist {
			rf.persist()
		}
		rf.mu.Unlock()
		return
	}
	baseIdx := rf.log[0].Idx
	if args.LastIncludedIdx <= baseIdx {
		if needPersist {
			rf.persist()
		}
		rf.mu.Unlock()
		return
	}
	rf.truncateLog(args.LastIncludedIdx, args.LastIncludedTerm)
	rf.commitIdx = args.LastIncludedIdx
	rf.lastApplied = args.LastIncludedIdx
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// 持久化当前的任期，投票的id，日志
	e.Encode(rf.curTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, args.Snapshot)
	msg := ApplyMsg{
		CommandValid: false,
		SnapShot:     args.Snapshot,
	}
	rf.mu.Unlock()
	rf.applyCh <- msg
}

func (rf *Raft) HandleInstallSnapShot(id int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server[%d] state curTerm %v install snapshot reply from server %v 's term %v",
		rf.me, stateString[rf.curState], id, reply.Term)

	// 收到rpc reply后leader退位或者term过期
	if rf.curState != Leader || args.Term != rf.curTerm {
		return
	}

	if reply.Term > rf.curTerm {
		rf.ChangeState(Follower)
		rf.curTerm = reply.Term
		rf.voteFor = -1
		rf.persist()
		return
	}
	rf.nextIdx[id] = args.LastIncludedIdx + 1
	rf.matchIdx[id] = rf.nextIdx[id] - 1
}

func (rf *Raft) SnapShot(idx int, snapshot []byte) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	baseIdx := rf.log[0].Idx
	DPrintf("server %v, mksnap idx..%v, baseIdx..%v, lastLogIdx..%d", rf.me, idx, baseIdx, rf.LastLogIdx())
	if idx <= baseIdx { // 已经打过快照的，不用再打快照
		DPrintf("server[%d] term %d baseIdx %d state %s cannot mksnap idx %d", rf.me, rf.curTerm, rf.commitIdx, stateString[rf.curState], idx)
		return
	}

	if idx > rf.commitIdx { // 未提交的日志不能打快照
		DPrintf("server[%d] term %d commitidx %d state %s cannot mksnap idx %d", rf.me, rf.curTerm, rf.commitIdx, stateString[rf.curState], idx)
		return
	}
	lastIncludedIdx := idx
	lastIncludedTerm := rf.log[idx-baseIdx].Term
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Term: lastIncludedTerm, Idx: lastIncludedIdx})

	for i := idx - baseIdx + 1; i < len(rf.log); i++ {
		newLog = append(newLog, rf.log[i])
	}
	DPrintf("[mksnap] %d term %d old log %v", rf.me, rf.curTerm, rf.log)
	rf.log = newLog
	DPrintf("[mksnap] %d term %d new log %v", rf.me, rf.curTerm, rf.log)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// 持久化当前的任期，投票的id，日志
	e.Encode(rf.curTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)

	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, snapshot)
}
