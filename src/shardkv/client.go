package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/src/labrpc"
	"6.824/src/shardmaster"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIds map[int]int
	clientId  int64
	cmdIdx    int64
}

// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.cmdIdx = 0
	ck.leaderIds = make(map[int]int)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		CmdIdx:   ck.cmdIdx,
	}
	ck.cmdIdx++

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			if _, ok = ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]
			newLeaderId := oldLeaderId
			for {
				var reply GetReply
				ok := ck.make_end(servers[newLeaderId]).Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					DPrintf("client %d get key %s success from server %d %v", ck.clientId, key, newLeaderId, reply.Err)
					ck.leaderIds[gid] = newLeaderId
					return reply.Value
				} else if ok && (reply.Err == ErrWrongGroup) {
					DPrintf("client %d get key %s wrong group %d server %d", ck.clientId, key, gid, newLeaderId)
					break
				} else {
					DPrintf("client %d get key %s wrong leader %d", ck.clientId, key, newLeaderId)
					newLeaderId = (newLeaderId + 1) % len(servers)
					if newLeaderId == oldLeaderId {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		CmdIdx:   ck.cmdIdx,
	}
	ck.cmdIdx++

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok = ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]
			newLeaderId := oldLeaderId
			for {
				var reply PutAppendReply
				DPrintf("[client %d --> server %d] PutAppend key %s value %s op %s",
					ck.clientId, newLeaderId, key, value, op)
				ok := ck.make_end(servers[newLeaderId]).Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					DPrintf("client %d PutAppend key %s value %s op %s to server %d success",
						ck.clientId, key, value, op, newLeaderId)
					ck.leaderIds[gid] = newLeaderId
					return
				} else if ok && reply.Err == ErrWrongGroup {
					DPrintf("client %d PutAppend key %s value %s op %s to server %d failed wrong group %d",
						ck.clientId, key, value, op, newLeaderId, gid)
					break
				} else {
					DPrintf("client %d PutAppend key %s value %s op %s to server %d failed wrong leader",
						ck.clientId, key, value, op, newLeaderId)
					newLeaderId = (newLeaderId + 1) % len(servers)
					if newLeaderId == oldLeaderId {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
