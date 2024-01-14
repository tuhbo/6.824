package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
	"github.com/zeromicro/go-zero/core/logx"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		logx.WithCallerSkip(1).Debugf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	ClientId int64
	CmdIdx   int64
	Cmd      string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db        map[string]string // 状态机
	waiter    map[int]chan Op   // 用于每条op提交log
	clientReq map[int64]int64   // 记录clientId已经完成的最新的请求ID
}

func (kv *KVServer) SubmitLog(op Op) bool {
	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("server %d is not leader op %s failed", kv.me, op.Cmd)
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.waiter[idx] // 创建一个waiter，用于等待raft提交log
	if !ok {
		ch = make(chan Op, 1)
		kv.waiter[idx] = ch
	}
	kv.mu.Unlock()

	// 同步等待raft提交log
	var ret bool
	select {
	case entry := <-ch:
		ret = (entry == op)
	case <-time.After(time.Millisecond * 1000): // 超时1s返回
		ret = false
	}

	kv.mu.Lock()
	delete(kv.waiter, idx)
	kv.mu.Unlock()
	return ret
}

func (kv *KVServer) ApplyState(op Op) {
	switch op.Cmd {
	case "Put":
		kv.db[op.Key] = op.Value
	case "Append":
		kv.db[op.Key] += op.Value
	}
}

func (kv *KVServer) IsStaleReq(clientId int64, cmdIdx int64) bool {
	completedCmdIdx, ok := kv.clientReq[clientId]
	if !ok {
		return false
	}
	return cmdIdx <= completedCmdIdx
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		Key:      args.Key,
		ClientId: args.ClientId,
		Cmd:      "Get",
		CmdIdx:   args.CmdIdx,
	}
	ok := kv.SubmitLog(op)
	if !ok {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}

	kv.mu.Lock()
	val, ok := kv.db[args.Key]
	kv.mu.Unlock()
	if !ok {
		reply.Err = ErrNoKey
		reply.Value = ""
		return
	}
	reply.Err = OK
	reply.Value = val
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		Cmd:      args.Op,
		CmdIdx:   args.CmdIdx,
	}

	ok := kv.SubmitLog(op)
	if !ok {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) Run() {
	for !kv.killed() {
		msg := <-kv.applyCh // raft集群已经提交log
		op := msg.Command.(Op)
		idx := msg.CommandIndex
		clientId := op.ClientId
		cmdIdx := op.CmdIdx

		kv.mu.Lock()
		if !kv.IsStaleReq(clientId, cmdIdx) { // 老的request不需要更新状态
			kv.ApplyState(op) // 更新state
			kv.clientReq[clientId] = cmdIdx
		}
		ch, ok := kv.waiter[idx]
		if ok {
			ch <- op // 唤醒rpc handle，回复client
		}
		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.waiter = make(map[int]chan Op)
	kv.clientReq = make(map[int64]int64)

	// You may need initialization code here.
	go kv.Run()
	return kv
}
