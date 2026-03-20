package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = false

var raftLogger *log.Logger

func init() {
	f, err := os.OpenFile("raft.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	raftLogger = log.New(f, "[RAFT] ", log.LstdFlags|log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		raftLogger.Printf(format, a...)
	}
	return
}