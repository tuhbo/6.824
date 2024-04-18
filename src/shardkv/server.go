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
		DPrintf("gid[%d] server[%d] is not leader event %s failed", kv.gid, kv.me, event)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("gid[%d] server[%d] append log idx %d event %s", kv.gid, kv.me, idx, event)

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
		reply.Err = res.Err
		reply.Value = res.Value
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
		reply.Err = lastReplyContext.Reply.Err
		kv.mu.Unlock()
		return
	}
	if !kv.canServe(key2shard(args.Key)) {
		DPrintf("gid[%d] server[%d] not server for key %s req %v", kv.gid, kv.me, args.Key, *args)
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
	return kv.curConf.Shards[shardId] == kv.gid &&
		(kv.stateMachines[shardId].Status == Active || kv.stateMachines[shardId].Status == Gcing)
}

func (kv *ShardKV) ApplyToStateMachine(shardId int, args *CommonClientReq) *CommonReply {
	reply := CommonReply{
		Err: OK,
	}
	shard, ok := kv.stateMachines[shardId]
	if !ok {
		if args.Op == OpGet {
			DPrintf("gid[%d] server[%d] shardId %d do not has key %v", kv.me, kv.gid, shardId, args)
			reply.Err = ErrNoKey
			return &reply
		}
		kv.stateMachines[shardId] = NewShard()
		shard = kv.stateMachines[shardId]
	}
	switch args.Op {
	case OpGet:
		if val, ok := shard.KV[args.Key]; !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = val
		}
		break
	case OpPut:
		shard.KV[args.Key] = args.Value
		break
	case OpAppend:
		shard.KV[args.Key] += args.Value
		break
	}
	return &reply
}

func (kv *ShardKV) ApplyClientReq(args *CommonClientReq) *CommonReply {
	shardId := key2shard(args.Key)
	if kv.canServe(shardId) {
		if args.Op != OpGet && kv.IsStaleReq(args.ClientId, args.ReqSeq) {
			DPrintf("gid[%d] server[%d] do not apply req %v to statemachine maxAppliedCmd %v",
				kv.gid, kv.me, args, kv.clientReq[args.ClientId])
			lastRely := kv.clientReq[args.ClientId].Reply
			return lastRely
		} else {
			reply := kv.ApplyToStateMachine(shardId, args)
			if args.Op != OpGet {
				kv.clientReq[args.ClientId] = ClientRequestContext{
					ReqSeq: args.ReqSeq,
					Reply:  reply,
				}
			}
			return reply
		}
	}
	DPrintf("gid[%d] server[%d] can not serve for req %v shardId %d config %v", kv.gid, kv.me, args, shardId, kv.curConf)
	return &CommonReply{
		Err: ErrWrongGroup,
	}
}

func GetGidShard(groupId int, conf *shardmaster.Config) map[int]int {
	shard := make(map[int]int)
	for shardId, gid := range conf.Shards {
		if gid == groupId {
			shard[shardId] = gid
		}
	}
	return shard
}

func (kv *ShardKV) GetShardByStatus(status ShardStatus) map[int][]int {
	res := make(map[int][]int)
	for shardId, shard := range kv.stateMachines {
		if shard.Status == status {
			gid := kv.lastConf.Shards[shardId]
			res[gid] = append(res[gid], shardId)
		}
	}
	return res
}

func (kv *ShardKV) UpdateShardStatus(newConf *shardmaster.Config) {
	if newConf.Num == 1 {
		for Id, _ := range newConf.Shards {
			kv.stateMachines[Id] = NewShard()
		}
		return
	}
	curShard := GetGidShard(kv.gid, &kv.curConf)
	newShard := GetGidShard(kv.gid, newConf)
	DPrintf("gid[%d] server[%d] curShard %v newShard %v", kv.gid, kv.me, curShard, newShard)
	for shardId, _ := range curShard {
		if _, ok := newShard[shardId]; !ok {
			DPrintf("gid[%d] server[%d] shard %d change state to BePulled", kv.gid, kv.me, shardId)
			kv.stateMachines[shardId].Status = BePulled
		}
	}

	for shardId, _ := range newShard {
		if _, ok := curShard[shardId]; !ok {
			DPrintf("gid[%d] server[%d] shard %d change state to Migrating", kv.gid, kv.me, shardId)
			kv.stateMachines[shardId] = NewShard()
			kv.stateMachines[shardId].Status = Migrating
		}
	}
}

func (kv *ShardKV) ApplyConfigUpdate(newConf *shardmaster.Config) *CommonReply {
	if newConf.Num == kv.curConf.Num+1 {
		DPrintf("gid[%d] server[%d] update newConf %v oldConf %v", kv.gid, kv.me, *newConf, kv.curConf)
		kv.UpdateShardStatus(newConf)
		kv.lastConf = kv.curConf
		kv.curConf = *newConf
		return &CommonReply{Err: OK}
	}
	return &CommonReply{Err: ErrOutDated}
}

func (kv *ShardKV) PullConf() {
	canPullConf := true
	kv.mu.Lock()
	for id, shard := range kv.stateMachines {
		if shard.Status != Active { // group中还有分片任务没有完成，不能更新配置
			DPrintf("gid[%d] server[%d] shardId %d status not active, do not pull new conf", kv.gid, kv.me, id)
			canPullConf = false
			break
		}
	}
	curConfNum := kv.curConf.Num
	kv.mu.Unlock()

	if canPullConf {
		newConf := kv.mck.Query(curConfNum + 1)
		DPrintf("gid[%d] server[%d] pulled conf %d newconf %v", kv.gid, kv.me, curConfNum+1, newConf.Num)
		if newConf.Num == curConfNum+1 {
			kv.SubmitLog(NewLogEvent(UpdateConfig, newConf), &CommonReply{})
		}
	}
}

func (kv *ShardKV) MigrateShardData(args *MigrateShardDataReq, reply *MigrateShardDataReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		DPrintf("gid[%d] server[%d] migrate %v shard data err wrong leader", kv.gid, kv.me, *args)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if args.ConfNum > kv.curConf.Num {
		reply.Err = ErrNotReady
		DPrintf("gid[%d] server[%d] confnum %d not ready for req %v", kv.gid, kv.me, kv.curConf.Num, *args)
		kv.mu.Unlock()
		return
	}

	reply.ShardData = make(map[int]map[string]string)
	for _, shardId := range args.Shards {
		reply.ShardData[shardId] = kv.stateMachines[shardId].deepCopy()
	}

	reply.ClientReq = make(map[int64]ClientRequestContext)
	for clientId, ctx := range kv.clientReq {
		reply.ClientReq[clientId] = ClientRequestContext{
			ReqSeq: ctx.ReqSeq,
			Reply: &CommonReply{
				Err:   ctx.Reply.Err,
				Value: ctx.Reply.Value,
			},
		}
	}
	reply.Err, reply.ConfNum = OK, kv.curConf.Num
	DPrintf("gid[%d] server[%d] complete migrate shard %v data", kv.gid, kv.me, *args)
	kv.mu.Unlock()

}

func (kv *ShardKV) PullShardData() {
	kv.mu.Lock()
	migratingShards := kv.GetShardByStatus(Migrating)
	var wg sync.WaitGroup
	for gid, shards := range migratingShards {
		wg.Add(1)
		go func(servers []string, shards []int, confNum int, lastGid int) {
			args := MigrateShardDataReq{
				ConfNum: confNum,
				Shards:  shards,
			}
			for i, server := range servers {
				var reply MigrateShardDataReply
				DPrintf("gid[%d] server[%d] start pull shards %v data from gid[%d] server[%d]", kv.gid, kv.me, shards, lastGid, i)
				ok := kv.make_end(server).Call("ShardKV.MigrateShardData", &args, &reply)
				if ok && reply.Err == OK {
					DPrintf("gid[%d] server[%d] get shards %v data reply %v", kv.gid, kv.me, shards, reply)
					kv.SubmitLog(NewLogEvent(MigrateShard, reply), &CommonReply{})
				}
			}
			wg.Done()
		}(kv.lastConf.Groups[gid], shards, kv.curConf.Num, gid)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) ApplyMigrateShardData(reply *MigrateShardDataReply) *CommonReply {
	if reply.ConfNum == kv.curConf.Num {
		for shardId, db := range reply.ShardData {
			shard := kv.stateMachines[shardId]
			if shard.Status == Migrating {
				for key, val := range db {
					shard.KV[key] = val
				}
				shard.Status = Gcing
			} else {
				DPrintf("gid[%d] server[%d] dup migrate shard data curConf num %d reply.ConfNum %d", kv.gid, kv.me, kv.curConf.Num, reply.ConfNum)
				break
			}
		}

		for clientId, ctx := range reply.ClientReq {
			if lastReply, ok := kv.clientReq[clientId]; !ok || lastReply.ReqSeq < ctx.ReqSeq {
				kv.clientReq[clientId] = ClientRequestContext{
					ReqSeq: ctx.ReqSeq,
					Reply: &CommonReply{
						Err:   ctx.Reply.Err,
						Value: ctx.Reply.Value,
					},
				}

			}
		}
		DPrintf("gid[%d] server[%d] apply migrate shard data curConf num %d reply.ConfNum %d complete states %v",
			kv.gid, kv.me, kv.curConf.Num, reply.ConfNum, kv.stateMachines)
		return &CommonReply{Err: OK}
	}
	DPrintf("gid[%d] server[%d] reject stale migrate shard data curConf num %d reply.ConfNum %d", kv.gid, kv.me, kv.curConf.Num, reply.ConfNum)
	return &CommonReply{Err: ErrOutDated}
}

func (kv *ShardKV) Daemon(callBack func(), timeout time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			callBack()
		}
		time.Sleep(timeout)
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
			kv.mu.Lock()
			idx := msg.CommandIndex
			event := msg.Command.(LogEvent)
			var reply *CommonReply
			switch event.Type {
			case ClientRequest:
				arg := event.Data.(CommonClientReq)
				reply = kv.ApplyClientReq(&arg)
			case UpdateConfig:
				newConf := event.Data.(shardmaster.Config)
				reply = kv.ApplyConfigUpdate(&newConf)
			case MigrateShard:
				migrateReply := event.Data.(MigrateShardDataReply)
				reply = kv.ApplyMigrateShardData(&migrateReply)
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
				ch <- reply // 唤醒rpc handle，回复client
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
	labgob.Register(LogEvent{})
	labgob.Register(CommonClientReq{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(MigrateShardDataReply{})

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

	go kv.Apply()
	go kv.Daemon(kv.PullConf, PollCfgTimeOut)
	go kv.Daemon(kv.PullShardData, MigrateTimeOut)

	return kv
}
