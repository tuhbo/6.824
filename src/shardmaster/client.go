package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/src/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
	cmdIdx   int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.cmdIdx = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:      num,
		ClientId: ck.clientId,
		CmdIdx:   ck.cmdIdx,
	}
	ck.cmdIdx++
	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply QueryReply
			DPrintf("clientid %d send Query cmdidx %d num %v to server[%d]", ck.clientId, ck.cmdIdx, num, i)
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("clientid %d Query cmdidx %d num %v to server[%d] success...", ck.clientId, ck.cmdIdx, num, i)
				return reply.Config
			}
			DPrintf("clientid %d Query cmdidx %d num %v to server[%d] failed...", ck.clientId, ck.cmdIdx, num, i)

		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers:  servers,
		ClientId: ck.clientId,
		CmdIdx:   ck.cmdIdx,
	}
	ck.cmdIdx++

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply JoinReply
			DPrintf("clientid %d send join cmdidx %d group %v to server[%d]", ck.clientId, ck.cmdIdx, servers, i)
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("clientid %d join cmdidx %d group %v to server[%d] success...", ck.clientId, ck.cmdIdx, servers, i)
				return
			}
			DPrintf("clientid %d join cmdidx %d group %v to server[%d] failed...", ck.clientId, ck.cmdIdx, servers, i)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:     gids,
		ClientId: ck.clientId,
		CmdIdx:   ck.cmdIdx,
	}
	ck.cmdIdx++

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply LeaveReply
			DPrintf("clientid %d send leave cmdidx %d gids %v to server[%d]", ck.clientId, ck.cmdIdx, gids, i)
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("clientid %d leave cmdidx %d gids %v to server[%d] success...", ck.clientId, ck.cmdIdx, gids, i)
				return
			}
			DPrintf("clientid %d leave cmdidx %d gids %v to server[%d] failed...", ck.clientId, ck.cmdIdx, gids, i)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		GID:      gid,
		Shard:    shard,
		ClientId: ck.clientId,
		CmdIdx:   ck.cmdIdx,
	}
	ck.cmdIdx++

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply MoveReply
			DPrintf("clientid %d send move cmdidx %d shard %d gid %d to server[%d]", ck.clientId, ck.cmdIdx, shard, gid, i)
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("clientid %d move cmdidx %d shard %d gid %d to server[%d] sucess...", ck.clientId, ck.cmdIdx, shard, gid, i)
				return
			}
			DPrintf("clientid %d move cmdidx %d shard %d gid %d to server[%d] failed...", ck.clientId, ck.cmdIdx, shard, gid, i)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
