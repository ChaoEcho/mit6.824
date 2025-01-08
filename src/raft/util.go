package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		// 添加一个时间戳
		timeStr := time.Now().Format("2006-01-02 15:04:05")
		log.Printf(timeStr+" "+format, a...)
	}
}
