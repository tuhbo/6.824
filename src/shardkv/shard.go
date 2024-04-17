package shardkv

import (
	"fmt"
)

type ShardStatus int

const (
	Active ShardStatus = iota
	Migrating
	BePulled
	Gcing
)

type Shard struct {
	Status ShardStatus
	KV     map[string]string
}

func NewShard() *Shard {
	return &Shard{
		Status: Active,
		KV:     make(map[string]string),
	}
}

func (shard *Shard) Get(key string) (string, Err) {
	if val, ok := shard.KV[key]; ok {
		return val, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key string, val string) Err {
	shard.KV[key] = val
	return OK
}

func (shard *Shard) Append(key string, val string) Err {
	shard.KV[key] += val
	return OK
}

func (shard *Shard) deepCopy() map[string]string {
	kv := make(map[string]string)
	for k, v := range shard.KV {
		kv[k] = v
	}
	return kv
}

func ShardStatusToString(s ShardStatus) string {
	switch s {
	case Active:
		return "Active"
	case Migrating:
		return "Migrating"
	case BePulled:
		return "BePulled"
	case Gcing:
		return "Gcing"
	}
	return "unknown"
}

func (Shard *Shard) String() string {
	return fmt.Sprintf("status %s kv %v", ShardStatusToString(Shard.Status), Shard.KV)
}
