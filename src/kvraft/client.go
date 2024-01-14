package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/src/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	leaderId int
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
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.cmdIdx = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		CmdIdx:   ck.cmdIdx,
	}
	leaderId := ck.leaderId
	ck.cmdIdx++
	for {
		reply := GetReply{}
		DPrintf("[client %d --> server %d] Get key %s", ck.clientId, leaderId, key)
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			DPrintf("[client %d --> server %d] get key %s failed",
				ck.clientId, leaderId, key)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			DPrintf("client %d get key %s from server %d", ck.clientId, key, leaderId)
			ck.leaderId = leaderId
			return reply.Value
		case ErrNoKey:
			DPrintf("client %d get key %s err, server %d not has", ck.clientId, key, leaderId)
			ck.leaderId = leaderId
			return ""
		case ErrWrongLeader:
			DPrintf("client %d get key %s wrong leader %d", ck.clientId, key, leaderId)
			leaderId = (leaderId + 1) % len(ck.servers)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		CmdIdx:   ck.cmdIdx,
	}
	leaderId := ck.leaderId
	ck.cmdIdx++
	for {
		reply := PutAppendReply{}
		DPrintf("[client %d --> server %d] PutAppend key %s value %s op %s",
			ck.clientId, leaderId, key, value, op)
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			DPrintf("[client %d --> server %d] PutAppend key %s value %s op %s failed",
				ck.clientId, leaderId, key, value, op)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			DPrintf("client %d PutAppend key %s value %s op %s to server %d success",
				ck.clientId, key, value, op, leaderId)
			ck.leaderId = leaderId
			return
		case ErrWrongLeader:
			DPrintf("client %d PutAppend key %s value %s op %s to server %d failed wrong leader",
				ck.clientId, key, value, op, ck.leaderId)
			leaderId = (leaderId + 1) % len(ck.servers)
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
