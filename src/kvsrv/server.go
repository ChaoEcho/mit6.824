package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.

	// 保存已经完成的任务ID
	completedTaskIDMap sync.Map

	// 存储KV的Map
	storeKVMap map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.storeKVMap[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.RequestType == RequestTypeNotice {
		kv.completedTaskIDMap.Delete(args.TaskID)
		return
	}

	v, ok := kv.completedTaskIDMap.Load(args.TaskID)
	if ok {
		reply.Value = v.(string)
		return
	}
	kv.mu.Lock()
	oldValue := kv.storeKVMap[args.Key]
	kv.storeKVMap[args.Key] = args.Value
	kv.mu.Unlock()

	reply.Value = oldValue
	kv.completedTaskIDMap.Store(args.TaskID, oldValue)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.RequestType == RequestTypeNotice {
		kv.completedTaskIDMap.Delete(args.TaskID)
		return
	}

	v, ok := kv.completedTaskIDMap.Load(args.TaskID)
	if ok {
		reply.Value = v.(string)
		return
	}
	kv.mu.Lock()
	oldValue := kv.storeKVMap[args.Key]
	kv.storeKVMap[args.Key] = oldValue + args.Value
	kv.mu.Unlock()

	reply.Value = oldValue
	kv.completedTaskIDMap.Store(args.TaskID, oldValue)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.storeKVMap = make(map[string]string, 32)

	return kv
}
