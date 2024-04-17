package shardkv

// import "../shardmaster"
import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
	"6.824/src/shardmaster"
)

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead          int32 // set by Kill()
	stateMachines map[int]*Shard
	clientReq     map[int64]ClientRequestContext //记录clientId已经完成的最新的请求ID
	waiter        map[int]chan *CommonReply      // 用于每条op提交log
	mck           *shardmaster.Clerk
	lastConf      shardmaster.Config
	curConf       shardmaster.Config
}

func (kv *ShardKV) SubmitLog(event LogEvent, reply *CommonReply) {
	idx, _, isLeader := kv.rf.Start(event)
	if !isLeader {
		DPrintf("server %d is not leader event %s failed", kv.me, event)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch, ok := kv.waiter[idx] // 创建一个waiter，用于等待raft提交log
	if !ok {
		ch = make(chan *CommonReply, 1)
		kv.waiter[idx] = ch
	}
	kv.mu.Unlock()

	// 同步等待raft提交log
	var ret bool
	var res *CommonReply
	timer := time.NewTimer(time.Millisecond * 1000)
	select {
	case res = <-ch:
		ret = true
	case <-timer.C: // 超时1s返回
		ret = false
	}

	kv.mu.Lock()
	delete(kv.waiter, idx)
	kv.mu.Unlock()
	timer.Stop()

	if !ret {
		reply.Err = ErrWrongLeader
	} else {
		reply = res
	}
}

func (kv *ShardKV) ApplyState(op Op) {
	switch op.Cmd {
	case "Put":
		kv.db[op.Key] = op.Value
	case "Append":
		kv.db[op.Key] += op.Value
	}
}

func (kv *ShardKV) IsStaleReq(clientId int64, cmdIdx int64) bool {
	reply, ok := kv.clientReq[clientId]
	if !ok {
		return false
	}
	return cmdIdx <= reply.ReqSeq
}

func (kv *ShardKV) CommonClientRequest(args *CommonClientReq, reply *CommonReply) {
	kv.mu.Lock()
	if args.Op != OpGet && kv.IsStaleReq(args.ClientId, args.ReqSeq) {
		lastReplyContext := kv.clientReq[args.ClientId]
		*reply = lastReplyContext.reply
		kv.mu.Unlock()
		return
	}
	if !kv.canServe(key2shard(args.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	kv.SubmitLog(NewLogEvent(ClientRequest, *args), reply)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

/* 辅助函数，读取已持久化的 snapshot */
func (kv *ShardKV) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var stateMachines map[int]*Shard
	var clientReq map[int64]ClientRequestContext

	if d.Decode(&stateMachines) != nil || d.Decode(&clientReq) != nil {
		DPrintf("readSnapshot err")
	} else {
		kv.stateMachines = stateMachines
		kv.clientReq = clientReq
	}
}

func (kv *ShardKV) canServe(shardId int) bool {
	return kv.curConf.Shards[shardId] == kv.gid
}

func (kv *ShardKV) checkCfgChange() (shardmaster.Config, bool) {
	config := kv.mck.Query(-1)
	if config.Num != kv.curConf.Num {
		return config, true
	}
	return config, false
}

func (kv *ShardKV) PollCfg() {
	for !kv.killed() {
		if cfg, changed := kv.checkCfgChange(); changed {
			kv.mu.Lock()
			kv.curConf = cfg
			kv.mu.Unlock()
		}
		time.Sleep(PollCfgTimeOut)
	}
}

func (kv *ShardKV) Apply() {
	for !kv.killed() {
		msg := <-kv.applyCh // raft集群已经提交log

		if !msg.CommandValid { // follower收到leader的快照
			r := bytes.NewBuffer(msg.SnapShot)
			d := gob.NewDecoder(r)

			kv.mu.Lock()
			kv.stateMachines = make(map[int]*Shard)
			kv.clientReq = make(map[int64]ClientRequestContext)
			d.Decode(&kv.stateMachines)
			d.Decode(&kv.clientReq)
			kv.mu.Unlock()
		} else {
			event := msg.Command.(LogEvent)
			idx := msg.CommandIndex
			clientId := op.ClientId
			cmdIdx := op.CmdIdx
			kv.mu.Lock()
			if !kv.IsStaleReq(clientId, cmdIdx) && kv.canServe(key2shard(op.Key)) { // 老的request不需要更新状态
				kv.ApplyState(op) // 更新state
				kv.clientReq[clientId] = cmdIdx
			}

			if kv.maxraftstate != -1 && kv.rf.StateSize() > kv.maxraftstate {
				// 快照信息包括kv.db 和 clientreq
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.stateMachines)
				e.Encode(kv.clientReq)

				snapshot := w.Bytes()
				kv.rf.SnapShot(idx, snapshot)
			}
			ch, ok := kv.waiter[idx]
			if ok {
				ch <- op // 唤醒rpc handle，回复client
			}
			kv.mu.Unlock()
		}
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.waiter = make(map[int]chan *CommonReply)
	kv.clientReq = make(map[int64]ClientRequestContext)
	kv.stateMachines = make(map[int]*Shard)
	kv.lastConf = shardmaster.Config{}
	kv.curConf = shardmaster.Config{}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readSnapshot(persister.ReadSnapshot())

	go kv.PollCfg()
	go kv.Apply()

	return kv
}
