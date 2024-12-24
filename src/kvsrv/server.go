package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

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
	completedTaskIDMap map[string]bool

	// 存储KV的Map
	storeKVMap map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("Server Get Request TaskID: %s", args.TaskID)

	if kv.completedTaskIDMap[args.TaskID] {
		reply.Status = ReplyStatusDuplicate
		DPrintf("Server Get Duplicate Response TaskID: %s", args.TaskID)
		return
	}

	reply.Value = kv.storeKVMap[args.Key]
	kv.completedTaskIDMap[args.TaskID] = true
	DPrintf("Server Get Success Response TaskID: %s", args.TaskID)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("Server Put Request TaskID: %s", args.TaskID)

	if kv.completedTaskIDMap[args.TaskID] {
		reply.Status = ReplyStatusDuplicate
		DPrintf("Server Put Duplicate Response TaskID: %s", args.TaskID)
		return
	}

	kv.storeKVMap[args.Key] = args.Value
	kv.completedTaskIDMap[args.TaskID] = true
	DPrintf("Server Put Success Response TaskID: %s", args.TaskID)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("Server Append Request TaskID: %s", args.TaskID)

	if kv.completedTaskIDMap[args.TaskID] {
		reply.Status = ReplyStatusDuplicate
		DPrintf("Server Append Duplicate Response TaskID: %s", args.TaskID)
		return
	}
	reply.Value = kv.storeKVMap[args.Key]
	kv.storeKVMap[args.Key] = kv.storeKVMap[args.Key] + args.Value
	kv.completedTaskIDMap[args.TaskID] = true
	DPrintf("Server Append Success Response TaskID: %s", args.TaskID)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.completedTaskIDMap = make(map[string]bool)
	kv.storeKVMap = make(map[string]string)

	return kv
}
