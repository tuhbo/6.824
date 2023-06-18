package mr

import (
	"fmt"
)

const (
	TaskMap      = 1
	TaskReduce   = 2
	TaskWait     = 3
	TaskEnd      = 4
	FixedTimeOut = 15
)

var Debug int

type MetaTask struct {
	FileName string
	FileIdx  int
}

type MapTask struct {
	MetaTask
	NReduce int
}

type ReduceTask struct {
	MetaTask
	PartIdx int
}

func DPrintf(format string, data ...interface{}) {
	if Debug > 0 {
		fmt.Printf(format, data...)
	}
}
