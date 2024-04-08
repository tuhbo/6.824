package shardmaster

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	dead      int32
	waiter    map[int]chan Op // 用于每条op提交log
	clientReq map[int64]int64 // 记录clientId已经完成的最新的请求ID
}

type Op struct {
	// Your data here.
	ClientId int64
	CmdIdx   int64
	Cmd      string
	Servers  map[int][]string // new GID -> servers mappings, for Join op
	GIDs     []int            // for leave op
}

func (sm *ShardMaster) SubmitLog(op Op) bool {
	idx, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		raft.DPrintf("server %d is not leader op %s failed", sm.me, op.Cmd)
		return false
	}

	sm.mu.Lock()
	ch, ok := sm.waiter[idx] // 创建一个waiter，用于等待raft提交log
	if !ok {
		ch = make(chan Op, 1)
		sm.waiter[idx] = ch
	}
	sm.mu.Unlock()

	// 同步等待raft提交log
	var ret bool
	timer := time.NewTimer(time.Millisecond * 1000)
	select {
	case entry := <-ch:
		if entry.ClientId != op.ClientId ||
			entry.Cmd != op.Cmd ||
			entry.CmdIdx != op.CmdIdx {
			ret = false
		} else {
			ret = true
		}
	case <-timer.C: // 超时1s返回
		ret = false
	}
	sm.mu.Lock()
	delete(sm.waiter, idx)
	sm.mu.Unlock()
	timer.Stop()
	return ret
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		ClientId: args.ClientId,
		Cmd:      "Join",
		CmdIdx:   args.CmdIdx,
		Servers:  args.Servers,
	}
	ok := sm.SubmitLog(op)
	reply.WrongLeader = false
	if !ok {
		reply.WrongLeader = true
		return
	}

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) Killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) JoinHandle(servers map[int][]string) {
	sm.mu.Lock()
	lastConfig := sm.configs[len(sm.configs)-1]

	sm.mu.Unlock()
}

func (sm *ShardMaster) ApplyState(op Op) {
	switch op.Cmd {
	case "Join":
		break
	case "Leave":
		break
	case "Move":
		break
	case "Query":
		break
	default:
		raft.DPrintf("[shardmaster] server %d unknow op %s", sm.me, op.Cmd)
	}
}

func (sm *ShardMaster) IsStaleReq(clientId int64, cmdIdx int64) bool {
	completedCmdIdx, ok := sm.clientReq[clientId]
	if !ok {
		return false
	}
	return cmdIdx <= completedCmdIdx
}

func (sm *ShardMaster) Run() {
	for !sm.Killed() {
		raft.DPrintf("[shardmaster] server %d run in...", sm.me)
		msg := <-sm.applyCh // raft集群已经提交log
		raft.DPrintf("[shardmaster] server %d run out...", sm.me)

		op := msg.Command.(Op)
		idx := msg.CommandIndex
		clientId := op.ClientId
		cmdIdx := op.CmdIdx
		sm.mu.Lock()
		if !sm.IsStaleReq(clientId, cmdIdx) { // 老的request不需要更新状态
			sm.ApplyState(op) // 更新state
			sm.clientReq[clientId] = cmdIdx
		}
		ch, ok := sm.waiter[idx]
		if ok {
			ch <- op // 唤醒rpc handle，回复client
		}
		sm.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	return sm
}
