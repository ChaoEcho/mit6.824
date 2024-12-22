package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
)

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// 获取任务
	task, err := callAssignTask(&AssignTaskArgs{}, &AssignTaskReply{})
	if err != nil {
		fmt.Printf("assign task failed\n")
		return
	}

	if task.TaskType == MapTask {
		// 执行map任务
		content, err := os.ReadFile(task.FileName[0])
		if err != nil {
			fmt.Printf("read file failed\n")
			return
		}
		resKvs := mapf(task.FileName[0], string(content))
		// 将结果写入文件
		fileName := fmt.Sprintf("mr-%d-%d", task.TaskId, ihash(resKvs[0].Key)%task.NReduce)
		// 以JSON格式写入文件
		ofile, err := os.Create(fileName)
		if err != nil {
			fmt.Printf("create file failed\n")
			return
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range resKvs {
			enc.Encode(kv)
		}
		ofile.Close()
		// 通知任务完成
		callDoneTask(&DoneTaskArgs{Task: task}, &DoneTaskReply{})
	} else if task.TaskType == ReduceTask {
		// 初始化map
		kvs := make(map[string][]string)
		fileNames := task.FileName
		// 读取所有中间文件
		for _, fileName := range fileNames {
			// 打开文件，并读取所有kv对
			ifile, err := os.Open(fileName)
			if err != nil {
				fmt.Printf("open file failed\n")
				return
			}
			dec := json.NewDecoder(ifile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
			}
			ifile.Close()
			// 对每个key，调用reducef
		}
		for key, values := range kvs {
			output := reducef(key, values)
			// 将结果写入文件
			fileName := fmt.Sprintf("mr-out-%d", ihash(key)%task.NReduce)
			ofile, err := os.Create(fileName)
			if err != nil {
				fmt.Printf("create file failed\n")
				return
			}
			fmt.Fprintf(ofile, "%v %v\n", key, output)
			ofile.Close()
		}
		// 通知任务完成
		callDoneTask(&DoneTaskArgs{Task: task}, &DoneTaskReply{})
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func callAssignTask(args *AssignTaskArgs, reply *AssignTaskReply) (Task, error) {
	ok := call("Coordinator.AssignTask", args, reply)
	if ok {
		fmt.Printf("assign task %v\n", reply.Task)
		return reply.Task, nil
	} else {
		return Task{}, fmt.Errorf("assign task failed")
	}
}

func callDoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {
	ok := call("Coordinator.DoneTask", args, reply)
	if ok {
		fmt.Printf("done task %v\n", args.Task)
		return nil
	} else {
		return fmt.Errorf("done task failed")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	sockname := myCoordinatorSock()
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
