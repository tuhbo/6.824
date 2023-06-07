package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapTask(mapf func(string, string) []KeyValue, mapTask MapTask) {
	DPrintf("doing MapTask, filename..%v, FileIdx..%v, NReduce..%d",
		mapTask.FileName, mapTask.FileIdx, mapTask.NReduce)
	file, err := os.Open((mapTask.FileName))
	if err != nil {
		log.Fatal("cannot open %v\n", mapTask.FileName)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("can not read %v\n", mapTask.FileName)
		return
	}
	file.Close()
	kva := mapf(mapTask.FileName, string(content))
	// sort.Sort(ByKey(kva))

	kvs := make([][]KeyValue, mapTask.NReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % mapTask.NReduce
		kvs[idx] = append(kvs[idx], kv)
	}

	reduceTasks := []ReduceTask{}
	for idx, kv := range kvs { //kvs中的单词键值对结果保存到mr-x-y中
		reduceTask := ReduceTask{
			MetaTask: mapTask.MetaTask,
			PartIdx:  idx,
		}
		reduceTasks = append(reduceTasks, reduceTask)

		outFile := "mr-" + strconv.Itoa(mapTask.FileIdx) + "-" + strconv.Itoa(idx)
		tempfile, err := ioutil.TempFile("", "mr-map-temp-*")
		if err != nil {
			log.Fatal("create temp file failed\n")
			return
		}
		enc := json.NewEncoder(tempfile)
		for _, item := range kv {
			err = enc.Encode(&item)
		}
		if err != nil {
			log.Fatal("encode error filename..%v\n", mapTask.FileName)
			tempfile.Close()
			return
		}
		tempfile.Close()
		os.Rename(tempfile.Name(), outFile)
	}
	DPrintf("MapTask to ReduceTask done, filename..%v, fileIdx..%v", mapTask.FileName, mapTask.FileIdx)
	CallTaskDone(TaskMap, mapTask, reduceTasks)
}

func doReduceTask(reducef func(string, []string) string, reduceTask []ReduceTask) {
	outname := "mr-out-" + strconv.Itoa(reduceTask[0].PartIdx)
	tempfile, err := ioutil.TempFile("", "mr-out*")
	if err != nil {
		log.Fatal("create temp file failed\n")
		return
	}
	defer tempfile.Close()

	kvs := []KeyValue{}
	for _, v := range reduceTask {
		DPrintf("doing ReduceTask, fileName..%v, fileIdx..%v, partIdx..%v\n", v.FileName, v.FileIdx, v.PartIdx)

		fileName := "mr-" + strconv.Itoa(v.FileIdx) + "-" + strconv.Itoa(v.PartIdx)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("cannot open %v\n", fileName)
			return
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}

	}

	sort.Sort(ByKey(kvs))

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempfile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
	tempName := tempfile.Name()
	os.Rename(tempName, outname)
	DPrintf("ReduceTask done, fileName..%v, fileIdx..%v\n", reduceTask[0].FileName, reduceTask[0].FileIdx)
	CallTaskDone(TaskReduce, MapTask{}, reduceTask)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		reply := CallAskTask()

		switch reply.TaskType {
		case TaskMap:
			doMapTask(mapf, reply.MapTask)
			break
		case TaskReduce:
			doReduceTask(reducef, reply.ReduceTask)
			break
		case TaskWait:
			break
		case TaskEnd:
			DPrintf("all task done!\n")
			return
		default:
			return
		}
	}
	// CallExample()
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallAskTask() AskTaskReply {
	askArgs := AskTaskArgs{}
	askReply := AskTaskReply{}

	call("Master.AskTask", &askArgs, &askReply)
	return askReply
}

func CallTaskDone(tasktype int, mapTask MapTask, reducesTasks []ReduceTask) {
	args := TaskDoneReply{
		TaskType:   tasktype,
		MapTask:    mapTask,
		ReduceTask: reducesTasks,
	}
	reply := ExampleReply{}

	call("Master.TaskDone", &args, &reply)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
