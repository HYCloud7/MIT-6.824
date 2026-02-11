package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
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

func RequstTask() TaskReply {
	TaskArgs := TaskArgs{}
	TaskReply := TaskReply{}

	ok := call("Coordinator.AssignTask", &TaskArgs, &TaskReply)

	if !ok {
		fmt.Println("Call failed!!!")
	}

	return TaskReply
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	loop := true
	for loop {
		TaskReply := RequstTask()
		switch TaskReply.TaskFlag {
		case TaskGetted:
			Task := TaskReply.Task
			switch Task.TaskType {
			case MapTask:
				doMapTask(mapf, &Task)
				FinishTaskAndReport(Task.TaskId)
			case ReduceTask:
				doReduceTask(reducef, &Task)
				FinishTaskAndReport(Task.TaskId)
			}
		case WaitPlz:
			time.Sleep(time.Second)
		case FinishedAndExit:
			loop = false
		default:
			fmt.Println("request task error!!!")
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue,
	Task *Task) {
	intermediate := []KeyValue{}
	for _, filename := range Task.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//创建中间输出文件
	rn := Task.ReduceNum
	for i := 0; i < rn; i++ {
		midFileName := "mr-" + strconv.Itoa(Task.TaskId) + "-" + strconv.Itoa(i)
		midFile, _ := os.Create(midFileName)
		enc := json.NewEncoder(midFile)
		for _, kv := range intermediate {
			if ihash(kv.Key)%rn == i {
				enc.Encode(&kv)
			}
		}
		midFile.Close()
	}
}

func doReduceTask(reducef func(string, []string) string,
	Task *Task) {

	intermediate := shuffle(Task.InputFiles)

	dir, _ := os.Getwd()
	tmpFile, err := os.CreateTemp(dir, "mr-out-tmpfile-")
	if err != nil {
		log.Fatal("failed to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tmpFile.Close()

	outName := "mr-out-" + strconv.Itoa(Task.Reducekth)

	os.Rename(dir+tmpFile.Name(), dir+outName)
}

func shuffle(InputFiles []string) []KeyValue {
	kva := []KeyValue{}
	for _, filename := range InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

func FinishTaskAndReport(id int) {
	args := FinArgs{TaskId: id}
	reply := FinReply{}

	// 请求调用Master的UpdateTaskState方法
	ok := call("Coordinator.UpdateTaskState", &args, &reply)
	if !ok { //请求失败
		fmt.Println("Call failed!")
	}
}

// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
