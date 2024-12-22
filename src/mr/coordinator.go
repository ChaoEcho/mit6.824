package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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
	// nReduce
	nReduce int
	// Coordinator的状态
	CoordinatorStatus CoordinatorStatus
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
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (c *Coordinator) AssignTask() Task {
	var task Task
	if c.CoordinatorStatus == CoordinatorMapStatus {
		// 从未开始任务列表中取一个任务
		task = c.UnstartedTaskList[0]
		// 将任务从未开始任务列表中移除
		c.UnstartedTaskList = append(c.UnstartedTaskList[:0], c.UnstartedTaskList[1:]...)
		// 将任务添加到正在执行任务列表中
		task.TaskStatus = TaskStatusRunning
		task.TaskStartTime = time.Now()
		c.RunningTaskList = append(c.RunningTaskList, task)
	} else if c.CoordinatorStatus == CoordinatorReduceStatus {
		task = c.RunningTaskList[0]
		c.RunningTaskList = append(c.RunningTaskList[:0], c.RunningTaskList[1:]...)
		task.TaskStatus = TaskStatusRunning
		task.TaskStartTime = time.Now()
		c.RunningTaskList = append(c.RunningTaskList, task)
	} else if c.CoordinatorStatus == CoordinatorDoneStatus {
		// 如果所有任务都已完成，则返回一个空的任务
		task = Task{}
	}
	return task
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// 初始化任务列表
	c.UnstartedTaskList = make([]Task, len(files))
	for i, file := range files {
		c.UnstartedTaskList[i] = Task{
			TaskType:   MapTask,
			FileName:   []string{file},
			TaskId:     i,
			TaskStatus: TaskStatusPending,
		}
	}

	c.nReduce = nReduce

	c.CoordinatorStatus = CoordinatorMapStatus

	// Your code here.
	// 创建一个协调者，然后来分配任务？

	c.server()
	return &c
}
