package raft

import (
	"github.com/zeromicro/go-zero/core/logx"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		logx.WithCallerSkip(1).Debugf(format, a...)
	}
	return
}
