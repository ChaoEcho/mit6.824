package mr

import (
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// 未开始任务列表
	UnstartedTaskList []Task
	// 正在执行任务列表
	RunningTaskList []Task
	// 已完成任务列表
	CompletedTaskList []Task
	// NReduce
	NReduce int
	// Coordinator的状态
	CoordinatorStatus CoordinatorStatus
	// 任务分配锁
	assignTaskMu sync.Mutex
	// 任务完成锁
	doneTaskMu sync.Mutex
}

type CoordinatorStatus int

const (
	CoordinatorMapStatus CoordinatorStatus = iota
	CoordinatorReduceStatus
	CoordinatorDoneStatus
)

type Task struct {
	TaskType TaskType
	// 文件名,使用数组是考虑到reduce阶段会有多个文件
	FileName []string
	// 任务id
	TaskId int
	// 任务状态
	TaskStatus TaskStatus
	// 任务开始时间
	TaskStartTime time.Time
	// NReduce
	NReduce int
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	EmptyTask
)

type TaskStatus int

const (
	TaskStatusPending TaskStatus = iota
	TaskStatusRunning
	TaskStatusCompleted
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	sockname := myCoordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	return c.CoordinatorStatus == CoordinatorDoneStatus
}

type DoneTaskArgs struct {
	Task Task
}

type DoneTaskReply struct {
}

// worker调用Done来标记任务完成，Coordianator则将任务添加到已完成任务列表中
func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {
	// 加锁
	c.doneTaskMu.Lock()
	c.assignTaskMu.Lock()
	defer c.assignTaskMu.Unlock()
	defer c.doneTaskMu.Unlock()
	// 遍历正在执行任务列表，找到对应任务，然后移除
	for i, task := range c.RunningTaskList {
		if task.TaskId == args.Task.TaskId {
			c.RunningTaskList = append(c.RunningTaskList[:i], c.RunningTaskList[i+1:]...)
			break
		}
	}
	// 将任务添加到已完成任务列表中
	c.CompletedTaskList = append(c.CompletedTaskList, args.Task)

	// 如果当前阶段是Map阶段，并且所有任务都已完成，则切换到Reduce阶段
	if c.CoordinatorStatus == CoordinatorMapStatus && len(c.RunningTaskList) == 0 && len(c.UnstartedTaskList) == 0 {
		c.CoordinatorStatus = CoordinatorReduceStatus
		// 初始化reduce任务列表
		c.UnstartedTaskList = make([]Task, c.NReduce)
		// fmt.Printf("NReduce: %d\n", c.NReduce)
		slog.Info(fmt.Sprintf("Coordinator status from Map to Reduce, NReduce: %d", c.NReduce))
		for i := 0; i < c.NReduce; i++ {
			// 所有中间文件命名为 mr-X-Y,其中X是任务id，Y是reduce任务id
			var allFiles []string

			allFiles, err := filepath.Glob("mr-*")
			if err != nil {
				slog.Error("Coordinator glob failed", "error", err)
				return err
			}
			// fmt.Printf("i: %d, allFiles: %v\n", i, allFiles)
			// 遍历所有中间文件，找到后缀为i的文件
			var files []string
			for _, file := range allFiles {
				if strings.HasSuffix(file, strconv.Itoa(i)) {
					files = append(files, file)
				}
			}
			slog.Info(fmt.Sprintf("Coordinator i: %d, files: %v", i, files))
			c.UnstartedTaskList[i] = Task{
				TaskType:   ReduceTask,
				TaskId:     generateTaskId(),
				TaskStatus: TaskStatusPending,
				FileName:   files,
				NReduce:    c.NReduce,
			}
		}
		slog.Info("Coordinator Status from Map to Reduce", "status", c.CoordinatorStatus)
		//fmt.Printf("Status from Map to Reduce: %v\n", c.CoordinatorStatus)
		//fmt.Printf("UnstartedTaskList: %v\n", c.UnstartedTaskList)
	} else if c.CoordinatorStatus == CoordinatorReduceStatus && len(c.RunningTaskList) == 0 && len(c.UnstartedTaskList) == 0 {
		slog.Info("Coordinator Status from Reduce to Done", "status", c.CoordinatorStatus)
		c.CoordinatorStatus = CoordinatorDoneStatus
	}

	return nil
}

// 分配任务
type AssignTaskArgs struct {
}

type AssignTaskReply struct {
	Task Task
}



func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	// 加锁
	c.assignTaskMu.Lock()
	defer c.assignTaskMu.Unlock()
	var task Task
	if c.CoordinatorStatus == CoordinatorMapStatus && len(c.UnstartedTaskList) > 0 {
		task = c.UnstartedTaskList[0]
		// fmt.Printf("Assign Map Task: %v\n", task)
		slog.Info(fmt.Sprintf("Coordinator Assign Map Task: %v", task))
		c.UnstartedTaskList = c.UnstartedTaskList[1:]
		task.TaskStatus = TaskStatusRunning
		task.TaskStartTime = time.Now()
		c.RunningTaskList = append(c.RunningTaskList, task)
	} else if c.CoordinatorStatus == CoordinatorReduceStatus && len(c.UnstartedTaskList) > 0 {
		// fmt.Printf("Assign Reduce Task: %v\n", c.UnstartedTaskList)
		task = c.UnstartedTaskList[0]
		// fmt.Printf("Assign Reduce Task: %v\n", task)
		slog.Info(fmt.Sprintf("Coordinator Assign Reduce Task: %v", task))
		c.UnstartedTaskList = c.UnstartedTaskList[1:]
		task.TaskStatus = TaskStatusRunning
		task.TaskStartTime = time.Now()
		c.RunningTaskList = append(c.RunningTaskList, task)
	} else if c.CoordinatorStatus == CoordinatorDoneStatus {
		// 如果所有任务都已完成，则返回空任务
		task = Task{
			TaskType: EmptyTask,
		}
	} else {
		task = Task{
			TaskType: WaitTask,
		}
	}
	reply.Task = task
	return nil
}

// 全局任务id
var taskId int = 0

var generateTaskIdMu sync.Mutex
func generateTaskId() int {
	generateTaskIdMu.Lock()
	defer generateTaskIdMu.Unlock()
	taskId++
	return taskId
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	//fmt.Printf("files: %v, nReduce: %d\n", files, nReduce)
	// 初始化任务列表
	c.UnstartedTaskList = make([]Task, len(files))
	for i, file := range files {
		c.UnstartedTaskList[i] = Task{
			TaskType:   MapTask,
			FileName:   []string{file},
			TaskId:     generateTaskId(),
			TaskStatus: TaskStatusPending,
			NReduce:    nReduce,
		}
		// fmt.Printf("UnstartedTaskList[%d]: %v\n", i, c.UnstartedTaskList[i])
	}

	c.NReduce = nReduce

	c.CoordinatorStatus = CoordinatorMapStatus

	// Your code here.
	// 创建一个协调者，然后来分配任务？

	c.server()
	return &c
}
