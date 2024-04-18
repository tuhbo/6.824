package shardkv

import (
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutDated    = "ErrOutDated"
	ErrNotReady    = "ErrNotReady"
)

const (
	PollCfgTimeOut = 50 * time.Millisecond
)

type Err string

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		logx.WithCallerSkip(1).Debugf(format, a...)
	}
	return
}

type ClientOp int

const (
	OpGet ClientOp = iota
	OpPut
	OpAppend
)

type CommonClientReq struct {
	Op       ClientOp
	ClientId int64
	ReqSeq   int64
	Key      string
	Value    string
}

func (req CommonClientReq) String() string {
	if req.Op == OpGet {
		return fmt.Sprintf("clientId %d Get request key %s  ReqSeq %d ", req.ClientId, req.Key, req.ReqSeq)
	} else if req.Op == OpPut {
		return fmt.Sprintf("clientId %d Put request key %s value %s ReqSeq %d ", req.ClientId, req.Key, req.Value, req.ReqSeq)
	}
	return fmt.Sprintf("clientId %d Append request key %s value %s ReqSeq %d ", req.ClientId, req.Key, req.Value, req.ReqSeq)
}

type CommonReply struct {
	Err   Err
	Value string
}

func ClientOpToString(Op ClientOp) string {
	switch Op {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"
	case OpAppend:
		return "Append"
	}
	return "unknown"
}

type ClientRequestContext struct {
	ReqSeq int64
	reply  *CommonReply
}

type LogEventType int

const (
	ClientRequest LogEventType = iota
	UpdateConfig
	MigrateShard
	DeleteShard
)

type LogEvent struct {
	Type LogEventType
	Data interface{}
}

func (event LogEvent) String() string {
	return fmt.Sprintf("LogEvent {type %v, Data %v}", event.Type, event.Data)
}

func NewLogEvent(t LogEventType, d interface{}) LogEvent {
	return LogEvent{
		Type: t,
		Data: d,
	}
}

type MigrateShardDataReq struct {
	ConfNum int
	Shards  []int
}

type MigrateShardDataReply struct {
	Err       Err
	ConfNum   int
	ShardData map[int]map[string]string
	ClientReq map[int64]ClientRequestContext
}
