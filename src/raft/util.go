package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = true
const Debug_level = 10000

func DPrintf_old(format string, a ...interface{}) {
	if Debug {
		// 添加一个时间戳
		//timeStr := time.Now().Format("2006-01-02 15:04:05")
		timeStr := GetCurrentTime("micro")
		log.Printf(timeStr+": "+format, a...)
	}
}

func DPrintf(level int, format string, a ...interface{}) (n int, err error) {
	if Debug_level <= level {
		log.Printf(format, a...)
	}
	return
}

func GetCurrentTime(precision string) string {
	now := time.Now()

	switch precision {
	case "micro":
		return now.Format("2006-01-02 15:04:05.000000")
	case "nano":
		return now.Format("2006-01-02 15:04:05.000000000")
	default:
		return now.Format("2006-01-02 15:04:05") // 默认秒精度
	}
}
