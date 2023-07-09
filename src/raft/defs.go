package raft

import (
	"math/rand"
	"time"
)

type Role int

const (
	NoOne            = -1
	Follower         = 0
	Candidate        = 1
	Leader           = 2
	ChanCap          = 100
	ElectionTimeOut  = time.Millisecond * 250 /* 要远大于论文中的 150-300 ms 才有意义，当然也要保证在 5 秒之内完成测试 */
	HeartBeatTimeOut = time.Millisecond * 100 /* 心跳 1 秒不超过 10 次 */
	OutRange         = -1
	Committed        = -2
)

/* 生成随机超时时间，在 250ms~500 ms 范围之内 */
func randElectionTimeOut() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	t := time.Duration(r.Int63()) % ElectionTimeOut
	return ElectionTimeOut + t
}

func (rf *Raft) lastLogIdx() int {
	return rf.entry[len(rf.entry)-1].Idx
}

func (rf *Raft) lastLogTerm() int {
	return rf.entry[len(rf.entry)-1].Term
}
