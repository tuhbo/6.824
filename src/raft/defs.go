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
	ElectionTimeOut  = time.Millisecond * 250
	HeartBeatTimeOut = time.Microsecond * 100 //心跳1秒不超过10次
	ChanCap          = 100
)

// 生成随机超时时间，在 250ms~500 ms 范围之内
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

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
