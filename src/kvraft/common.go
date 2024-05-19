package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const replyTimeout = time.Duration(100) * time.Millisecond
const retryTimeout = time.Duration(10) * time.Millisecond


// Put or Append
type OpAppendArgs struct {
	Key   		string
	Value 		string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId 	int64
	Seq      	int64
	Op			string
}

type OpAppendReply struct {
	Err Err
	Value string
}
