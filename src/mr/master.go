package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	mtx           sync.Mutex
	mapTaskQ      []MapTask    // 待分配给worker的map任务队列
	reduceTaskQ   []ReduceTask // 待分配给woker的reduce任务队列
	mapTaskingQ   []MapTask    // 已经分配给woekr的map任务队列
	reducingTaskQ []ReduceTask // 已经分配给woker的reduce任务队列
	nReduce       int
	isDone        bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func init() {
	Debug = 0

	///* bash -c 表示脚本命令为接着其后的字符串内容，即 "rm -rf mr-*" 且不容忽略通配符* */
	// cmd := exec.Command("/bin/bash", "-c", "rm -rf mr-*")
	// err := cmd.Run() /* 删掉上次运行的 mr-out 缓存文件 */
	// if err != nil {
	// 	log.Fatal("cmd.Run() failed with %v", err)
	// }
}

func (m *Master) assignMapTask() MapTask {
	task := m.mapTaskQ[0]
	m.mapTaskQ = append(m.mapTaskQ[:0], m.mapTaskQ[1:]...)
	m.mapTaskingQ = append(m.mapTaskingQ, task)

	DPrintf("assign MapTask, fileName..%v, fileIdx..%v, nReduce..%v\n", task.FileName, task.FileIdx, task.NReduce)

	return task
}

func (m *Master) assignReduceTask() []ReduceTask {
	reduceTasks := make([]ReduceTask, 0)
	partIdx := m.reduceTaskQ[0].PartIdx

	for i := 0; i < len(m.reduceTaskQ); {
		if m.reduceTaskQ[i].PartIdx != partIdx {
			i++
			continue
		}

		// 找到编号为partIdx的reduceTask
		task := m.reduceTaskQ[i]
		m.reducingTaskQ = append(m.reducingTaskQ, task)
		reduceTasks = append(reduceTasks, task)
		m.reduceTaskQ = append(m.reduceTaskQ[:i], m.reduceTaskQ[i+1:]...)
		DPrintf("assign ReduceTask, fileName..%v, fileIdx..%v, partIdx..%v\n",
			task.FileName, task.FileIdx, task.PartIdx)
	}
	return reduceTasks
}

func (m *Master) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if len(m.mapTaskQ) > 0 {
		reply.TaskType = TaskMap
		reply.MapTask = m.assignMapTask()
		DPrintf("assign mapTask done\n")
		return nil
	}

	if len(m.mapTaskingQ) > 0 {
		reply.TaskType = TaskWait
		DPrintf("some MapTask have not been completed, please wait...\n")
		return nil
	}

	//当所有的map task完成后，才进行reduce任务
	if len(m.reduceTaskQ) > 0 {
		reply.TaskType = TaskReduce
		reduceTasks := m.assignReduceTask()
		reply.ReduceTask = append(reply.ReduceTask, reduceTasks...)
		DPrintf("assign reduceTask done\n")
		return nil
	}

	if len(m.reducingTaskQ) > 0 {
		reply.TaskType = TaskWait
		DPrintf("some ReduceTask are not completed, please wait...\n")
		return nil
	}
	reply.TaskType = TaskEnd
	m.isDone = true
	DPrintf("All tasks done, msg: closeing -> worker...\n")
	return nil
}

func (m *Master) mapTaskingQDeleter(mapTask MapTask, reduceTasks []ReduceTask) {
	DPrintf("MapTask Done, fileName..%v, fileIdx..%v, nReduce..%v\n", mapTask.FileName, mapTask.FileIdx, mapTask.NReduce)

	for i := 0; i < len(m.mapTaskingQ); i++ {
		if m.mapTaskingQ[i].FileIdx == mapTask.FileIdx {
			m.mapTaskingQ = append(m.mapTaskingQ[:i], m.mapTaskingQ[i+1:]...)
			break
		}
	}

	for _, v := range reduceTasks {
		m.reduceTaskQ = append(m.reduceTaskQ, v)
		DPrintf("add ReduceTask, fileName..%v, fileIdx..%v, partIdx%v\n", v.FileName, v.FileIdx, v.PartIdx)
	}
}

func (m *Master) reduceTaskingQDeleter(reduceTasks []ReduceTask) {
	for i := 0; i < len(reduceTasks); i++ {
		task := reduceTasks[i]
		DPrintf("ReduceTask done, fileName..%v, fileIdx..%v, partIdx..%v\n", task.FileName, task.FileIdx, task.PartIdx)

		for j := 0; j < len(m.reducingTaskQ); {
			if m.reducingTaskQ[j].PartIdx == task.PartIdx {
				m.reducingTaskQ = append(m.reducingTaskQ[:j], m.reducingTaskQ[j+1:]...)
				continue
			}
			j++
		}
	}
}

func (m *Master) TaskDone(args *TaskDoneReply, reply *ExampleReply) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	switch args.TaskType {
	case TaskMap:
		m.mapTaskingQDeleter(args.MapTask, args.ReduceTask)
		break
	case TaskReduce:
		m.reduceTaskingQDeleter(args.ReduceTask)
		break
	default:
		break
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	return m.isDone
}

func (m *Master) Poller() {
	for {
		time.Sleep(FixedTimeOut * time.Second)
		m.mtx.Lock()
		if len(m.mapTaskingQ) != 0 {
			for i, _ := range m.mapTaskingQ {
				m.mapTaskQ = append(m.mapTaskQ, m.mapTaskingQ[i])
			}
			m.mapTaskingQ = []MapTask{}
		}

		if len(m.reducingTaskQ) != 0 {
			DPrintf("redTaskingQ..%v\n", m.reducingTaskQ)
			for i, _ := range m.reducingTaskQ {
				m.reduceTaskQ = append(m.reduceTaskQ, m.reducingTaskQ[i])
			}
			m.reducingTaskQ = []ReduceTask{}
		}
		m.mtx.Unlock()
	}
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.mapTaskingQ = make([]MapTask, 0)
	m.nReduce = nReduce
	for i, file := range files {
		task := MapTask{
			MetaTask: MetaTask{
				FileName: file,
				FileIdx:  i,
			},
			NReduce: nReduce,
		}
		m.mapTaskQ = append(m.mapTaskQ, task)
	}
	m.isDone = false
	DPrintf("Master working....\n")

	go m.Poller()
	m.server()
	return &m
}
