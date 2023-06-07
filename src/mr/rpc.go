package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// woker向master询问要任务
type AskTaskArgs struct{}

// master向worker回复任务
type AskTaskReply struct {
	TaskType   int
	MapTask    MapTask
	ReduceTask []ReduceTask
}

// worker task done
type TaskDoneReply struct {
	TaskType   int
	MapTask    MapTask
	ReduceTask []ReduceTask
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
