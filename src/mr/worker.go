package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"log/slog"
	"net/rpc"
	"os"
	"time"
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
	
	time.Sleep(time.Microsecond*3)
	// 根据时间戳生成id，只保留后面4位数字
	nowId := time.Now().UnixNano() % 10000

	// Your worker implementation here.

	for {
		// 获取任务
		task, err := callAssignTask(&AssignTaskArgs{}, &AssignTaskReply{})
		if err != nil {
			slog.Error(fmt.Sprintf("Worker %d assign task failed", nowId), "error", err)
			return
		}

		// 如果所有任务都已完成，则退出
		if task.TaskType == EmptyTask {
			slog.Info(fmt.Sprintf("Worker %d all task done", nowId))
			break
		} else if task.TaskType == WaitTask {
			slog.Info(fmt.Sprintf("Worker %d sleep for task", nowId))
			time.Sleep(time.Second)
			continue
		} else if task.TaskType == MapTask {
			// 执行map任务
			slog.Info(fmt.Sprintf("Worker %d do map task", nowId), "task", task)
			content, err := os.ReadFile(task.FileName[0])
			if err != nil {
				slog.Error(fmt.Sprintf("Worker %d read file failed", nowId), "error", err)
				return
			}
			resKvs := mapf(task.FileName[0], string(content))

			// 遍历resKvs，先按照hash分类，加入到列表，然后批量写入文件
			kvs := make(map[int][]KeyValue)
			for _, kv := range resKvs {
				hash := ihash(kv.Key) % task.NReduce
				kvs[hash] = append(kvs[hash], kv)
			}

			for hash, kvs := range kvs {
				fileName := fmt.Sprintf("mr-%d-%d", task.TaskId, hash)
				ofile, err := os.Create(fileName)
				if err != nil {
					fmt.Printf("create file failed\n")
					return
				}
				enc := json.NewEncoder(ofile)
				for _, kv := range kvs {
					enc.Encode(kv)
				}
				ofile.Close()
			}
			// 通知任务完成
			callDoneTask(&DoneTaskArgs{Task: task}, &DoneTaskReply{})
		} else if task.TaskType == ReduceTask {
			// 初始化map
			slog.Info(fmt.Sprintf("Worker %d do reduce task", nowId), "task", task)
			kvs := make(map[string][]string)
			fileNames := task.FileName
			// 读取所有中间文件
			for _, fileName := range fileNames {
				// 打开文件，并读取所有kv对
				ifile, err := os.Open(fileName)
				if err != nil {
					slog.Error(fmt.Sprintf("Worker %d open file failed", nowId), "error", err)
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
			// 随机获取一个key，然后取hash
			var key string
			for k := range kvs {
				key = k
				break
			}
			fileName := fmt.Sprintf("mr-out-%d", ihash(key)%task.NReduce)
			ofile, err := os.Create(fileName)
			if err != nil {
				slog.Error(fmt.Sprintf("Worker %d create file failed", nowId), "error", err)
				return
			}
			for key, values := range kvs {
				output := reducef(key, values)
				// 将结果写入文件
				if _, err := ofile.WriteString(fmt.Sprintf("%v %v\n", key, output)); err != nil {
					slog.Error(fmt.Sprintf("Worker %d write file failed", nowId), "error", err)
					return
				}
				// 追加模式写入文件
				// ofile.WriteString(fmt.Sprintf("%v %v\n", key, output))
				// fmt.Fprintf(ofile, "%v %v\n", key, output)
			}
			ofile.Close()
			// 通知任务完成
			callDoneTask(&DoneTaskArgs{Task: task}, &DoneTaskReply{})
		}
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
		// fmt.Printf("assign task %v\n", reply.Task)
		return reply.Task, nil
	} else {
		return Task{}, fmt.Errorf("assign task failed")
	}
}

func callDoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {
	ok := call("Coordinator.DoneTask", args, reply)
	if ok {
		// fmt.Printf("done task %v\n", args.Task)
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
