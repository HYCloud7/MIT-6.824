package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mu sync.Mutex

type TaskType int // 任务类型

type TaskState int // 任务状态

type CurrentPhase int // 执行阶段

const (
	MapTask TaskType = iota
	ReduceTask
)

const (
	Waiting TaskState = iota
	Working
	Finished
)

const (
	MapPhase CurrentPhase = iota
	ReducePhase
	AllDone
)

type Task struct {
	TaskId     int
	TaskType   TaskType
	TaskState  TaskState
	InputFiles []string
	StartTime  time.Time
	ReduceNum  int
	Reducekth  int
}

type Coordinator struct {
	// Your definitions here.
	CurrentPhase      CurrentPhase
	TaskIdForGen      int
	MapTaskChannel    chan *Task
	ReduceTaskChannel chan *Task
	TaskMap           map[int]*Task // 存储已分配的任务
	MapNumber         int
	ReduceNumber      int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) UpdateTaskState(args *FinArgs, reply *FinReply) error {
	mu.Lock()
	defer mu.Unlock()

	id := args.TaskId
	
	c.TaskMap[id].TaskState = Finished

	return nil
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	// 多个节点都会向从coordinator请求分配任务
	mu.Lock()
	defer mu.Unlock()

	switch c.CurrentPhase {
	case MapPhase:
		if len(c.MapTaskChannel) > 0 {
			task := <-c.MapTaskChannel
			task.TaskState = Working
			task.StartTime = time.Now()
			reply.Task = *task
			reply.TaskFlag = TaskGetted
			c.TaskMap[(*task).TaskId] = task
		} else { // Map任务分配完
			reply.TaskFlag = WaitPlz
			if c.checkMapTaskDone() {
				c.toNextPhase()
			}
		}
	case ReducePhase:
		if len(c.ReduceTaskChannel) > 0 {
			task := <-c.ReduceTaskChannel
			task.TaskState = Working
			task.StartTime = time.Now()
			reply.Task = *task
			reply.TaskFlag = TaskGetted
			c.TaskMap[(*task).TaskId] = task
		} else {
			reply.TaskFlag = WaitPlz
			if c.checkReduceTaskDone() {
				c.toNextPhase()
			}
		}
	case AllDone:
		reply.TaskFlag = FinishedAndExit
	default:
		panic("Unknown Phase!!!")
	}
	return nil
}

func (c *Coordinator) checkMapTaskDone() bool {
	ret := false
	var (
		mapDoneNum   = 0
		mapUndoneNum = 0
	)
	for _, v := range c.TaskMap {
		if v.TaskType == MapTask {
			if v.TaskState == Finished {
				mapDoneNum++
			} else {
				mapUndoneNum++
			}
		}
	}
	if mapDoneNum == c.MapNumber && mapUndoneNum == 0 {
		ret = true
	}

	return ret
}

func (c *Coordinator) checkReduceTaskDone() bool {
	ret := false
	var (
		reduceDoneNum    = 0
		reuduceUndoneNum = 0
	)
	for _, v := range c.TaskMap {
		if v.TaskType == ReduceTask {
			if v.TaskState == Finished {
				reduceDoneNum++
			} else {
				reuduceUndoneNum++
			}
		}
	}
	if reduceDoneNum == c.ReduceNumber && reuduceUndoneNum == 0 {
		ret = true
	}

	return ret
}

func (c *Coordinator) toNextPhase() {
	switch c.CurrentPhase {
	case MapPhase:
		c.CurrentPhase = ReducePhase
		c.makeReduceTask()
	case ReducePhase:
		c.CurrentPhase = AllDone
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	mu.Lock()
	defer mu.Unlock()

	if c.CurrentPhase == AllDone {
		ret = true
	}

	return ret
}

func (c *Coordinator) GenerateTaskId() int {
	ret := c.TaskIdForGen
	c.TaskIdForGen++
	return ret
}

func (c *Coordinator) makeMapTask(files []string) {
	for _, file := range files {
		id := c.GenerateTaskId()
		task := Task{
			TaskId:     id,
			TaskType:   MapTask,
			TaskState:  Waiting,
			InputFiles: []string{file},
			ReduceNum:  c.ReduceNumber,
		}
		c.MapTaskChannel <- &task
	}
}

func (c *Coordinator) makeReduceTask() {
	nr := c.ReduceNumber

	dir, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < nr; i++ {
		id := c.GenerateTaskId()
		input := []string{}

		for _, file := range files {
			if strings.HasPrefix(file.Name(), "mr-") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
				input = append(input, file.Name())
			}
		}

		task := Task{
			TaskId:     id,
			TaskType:   ReduceTask,
			TaskState:  Waiting,
			InputFiles: input,
			ReduceNum:  nr,
			Reducekth:  i,
		}

		c.ReduceTaskChannel <- &task
	}
}

func (c *Coordinator) checkCrash() {
	for {
		time.Sleep(time.Second)
		mu.Lock()
		if c.CurrentPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, task := range c.TaskMap {
			if task.TaskState == Working && time.Since(task.StartTime) > 10 * time.Second {
				task.TaskState = Waiting
				switch task.TaskType {
				case MapTask:
					c.MapTaskChannel <- task
				case ReduceTask:
					c.ReduceTaskChannel <- task
				}
				delete(c.TaskMap, task.TaskId)
			}
		}
		mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		CurrentPhase:      MapPhase,
		TaskIdForGen:      0,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		TaskMap:           make(map[int]*Task, len(files)+nReduce),
		MapNumber:         len(files),
		ReduceNumber:      nReduce,
	}

	c.makeMapTask(files)

	c.server()

	go c.checkCrash()
	return &c
}
