package shardmaster

import (
	"sort"
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
	GID      int              // for move op
	Shard    int              // for move op
	Num      int              // for query op
	Conf     Config
}

func (sm *ShardMaster) SubmitLog(op Op) (bool, Op) {
	idx, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		DPrintf("server %d is not leader op %s failed", sm.me, op.Cmd)
		return false, Op{}
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
	var entry Op
	timer := time.NewTimer(time.Millisecond * 1000)
	select {
	case entry = <-ch:
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
	return ret, entry
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		ClientId: args.ClientId,
		Cmd:      "Join",
		CmdIdx:   args.CmdIdx,
		Servers:  args.Servers,
	}
	ok, _ := sm.SubmitLog(op)
	reply.WrongLeader = false
	if !ok {
		reply.WrongLeader = true
		return
	}
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		ClientId: args.ClientId,
		Cmd:      "Leave",
		CmdIdx:   args.CmdIdx,
		GIDs:     args.GIDs,
	}
	ok, _ := sm.SubmitLog(op)
	reply.WrongLeader = false
	if !ok {
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		ClientId: args.ClientId,
		Cmd:      "Move",
		CmdIdx:   args.CmdIdx,
		GID:      args.GID,
		Shard:    args.Shard,
	}
	ok, _ := sm.SubmitLog(op)
	reply.WrongLeader = false
	if !ok {
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	if args.Num >= 0 && args.Num < len(sm.configs) {
		reply.WrongLeader = false
		reply.Config.Num = sm.configs[args.Num].Num
		reply.Config.Shards = sm.configs[args.Num].Shards
		reply.Config.Groups = deepCopy(sm.configs[args.Num].Groups)
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	op := Op{
		ClientId: args.ClientId,
		Cmd:      "Query",
		CmdIdx:   args.CmdIdx,
		Num:      args.Num,
	}
	ok, entry := sm.SubmitLog(op)
	reply.WrongLeader = false
	if !ok {
		reply.WrongLeader = true
		return
	}
	config := entry.Conf
	reply.Config = Config{
		Num:    config.Num,
		Shards: config.Shards,
		Groups: deepCopy(config.Groups),
	}
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

func GetGroup2Shard(config Config) map[int][]int {
	g2s := make(map[int][]int, len(config.Groups))
	for gid, _ := range config.Groups {
		g2s[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		g2s[gid] = append(g2s[gid], shard)
	}
	return g2s
}

func GetGidWithMinShards(g2s map[int][]int) int {
	var gids []int
	for gid, _ := range g2s {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	DPrintf("g2s %v gids %v", g2s, gids)
	res, min := -1, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(g2s[gid]) < min {
			res, min = gid, len(g2s[gid])
		}
	}
	return res
}

func GetGidWithMaxShards(g2s map[int][]int) int {
	// 0号group，有shards，返回0号group
	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		DPrintf("0 group has shards %v", g2s)
		return 0
	}
	var gids []int
	for gid, _ := range g2s {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	res, max := -1, -1
	for _, gid := range gids {
		if len(g2s[gid]) > max {
			res, max = gid, len(g2s[gid])
		}
	}
	return res
}

func (sm *ShardMaster) JoinHandle(groups map[int][]string) {
	lastConfig := sm.configs[len(sm.configs)-1]
	DPrintf("[shardMaste] server %d join handle lastConfig %v add groups %v", sm.me, lastConfig, groups)
	// 生成新的配置
	newConfig := Config{
		Num:    len(sm.configs),
		Shards: lastConfig.Shards,
		Groups: deepCopy(lastConfig.Groups),
	}

	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	// 从shard最多的group移动一个shard到分片最少的group
	g2s := GetGroup2Shard(newConfig)
	for {
		source := GetGidWithMaxShards(g2s)
		target := GetGidWithMinShards(g2s)
		DPrintf("source %d target %d", source, target)
		if source != 0 && len(g2s[source])-len(g2s[target]) <= 1 {
			break
		}
		g2s[target] = append(g2s[target], g2s[source][0])
		g2s[source] = g2s[source][1:]
	}
	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) LeaveHandle(gids []int) {
	lastConfig := sm.configs[len(sm.configs)-1]
	DPrintf("[shardMaste] server %d leave handle"+
		" lastConfig %v leave gids %v",
		sm.me, lastConfig, gids)
	// 生成新的配置
	newConfig := Config{
		Num:    len(sm.configs),
		Shards: lastConfig.Shards,
		Groups: deepCopy(lastConfig.Groups),
	}

	g2s := GetGroup2Shard(newConfig)
	// 记录删除的group有哪些shard
	var deletedGroupShards []int
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}

		if shards, ok := g2s[gid]; ok {
			deletedGroupShards = append(deletedGroupShards, shards...)
			delete(g2s, gid)
		}
	}

	var newShards [NShards]int
	// 如果集群没有raft group，那么所有shard都归属于0 group
	if len(newConfig.Groups) != 0 {
		for _, shard := range deletedGroupShards {
			target := GetGidWithMinShards(g2s)
			g2s[target] = append(g2s[target], shard)
		}
		for gid, shards := range g2s {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) MoveHandle(Gid int, Shard int) {
	lastConfig := sm.configs[len(sm.configs)-1]
	DPrintf("[shardMaste] server %d move handle"+
		" lastConfig %v move shard %d to gid %d",
		sm.me, lastConfig, Shard, Gid)
	// 生成新的配置
	newConfig := Config{
		Num:    len(sm.configs),
		Shards: lastConfig.Shards,
		Groups: deepCopy(lastConfig.Groups),
	}

	var newShards [NShards]int = newConfig.Shards
	newShards[Shard] = Gid
	newConfig.Shards = newShards
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) QueryHandle(op *Op) {
	num := op.Num
	if num == -1 || num >= len(sm.configs) {
		num = len(sm.configs) - 1
	}
	config := sm.configs[num]
	op.Conf = Config{
		Num:    config.Num,
		Shards: config.Shards,
		Groups: deepCopy(config.Groups),
	}
}

func (sm *ShardMaster) ApplyState(op *Op) {
	switch op.Cmd {
	case "Join":
		sm.JoinHandle(op.Servers)
	case "Leave":
		sm.LeaveHandle(op.GIDs)
	case "Move":
		sm.MoveHandle(op.GID, op.Shard)
	case "Query":
		sm.QueryHandle(op)
	default:
		DPrintf("[shardmaster] server %d unknow op %s", sm.me, op.Cmd)
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
		DPrintf("[shardmaster] server %d run in...", sm.me)
		msg := <-sm.applyCh // raft集群已经提交log
		DPrintf("[shardmaster] server %d run out...", sm.me)

		op := msg.Command.(Op)
		idx := msg.CommandIndex
		clientId := op.ClientId
		cmdIdx := op.CmdIdx
		sm.mu.Lock()
		if !sm.IsStaleReq(clientId, cmdIdx) { // 老的request不需要更新状态
			sm.ApplyState(&op) // 更新state
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
	sm.waiter = make(map[int]chan Op)
	sm.clientReq = make(map[int64]int64)
	// Your code here.
	go sm.Run()

	return sm
}
