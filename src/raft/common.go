package raft

import (
	"math/rand"
	"time"
)

type State int

const (
	Follower = iota
	Candidate
	Leader
)

var stateString = []string{
	"Follower",
	"Candidate",
	"Leader",
}

const ElectionTimeOut = time.Millisecond * 250

const HeartBeatTimeOut = time.Millisecond * 100

const (
	IdxOutRange = -1
	IdxCommited = -2
)

/* 生成随机超时时间，在 250ms~500 ms 范围之内 */
func randElectionTimeOut() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	t := time.Duration(r.Int63()) % ElectionTimeOut
	return ElectionTimeOut + t
}

func ResetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C: // try to drain the channel
		default:
		}
	}
	timer.Reset(d)
}
