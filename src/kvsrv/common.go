package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// 任务的唯一ID
	TaskID string
}

type PutAppendReply struct {
	Value  string
	Status ReplyStatus
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	// 任务的唯一ID
	TaskID string
}

type GetReply struct {
	Value  string
	Status ReplyStatus
}

type ReplyStatus int

const (
	ReplyStatusSuccess ReplyStatus = iota
	ReplyStatusFailed
	ReplyStatusDuplicate
)
