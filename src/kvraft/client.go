package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId    int64 // client的唯一标识
	knownLeader int   // 已知的leader，请求到非leader的server后的reply中获取
	commandNum  int   // 标志client为每个command分配的序列号到哪了（采用自增，从1开始）
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand() // clientId通过提供的nrand()随机生成，重复概率忽略不计
	ck.knownLeader = -1   // 开始不知道谁是leader，初始为-1
	ck.commandNum = 1

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		CmdNum:   ck.commandNum,
	}

	i := 0 // 从server 0开始发送请求
	serversNum := len(ck.servers)
	for ; ; i = (i + 1) % serversNum { // 依次循环访问每一个server直到找到leader

		// 如果找到了leader
		if ck.knownLeader != -1 {
			i = ck.knownLeader
		} else {
			time.Sleep(time.Millisecond * 5)
		}

		reply := GetReply{}

		DPrintf("Client[%d] request [%d] for Get(key:%v)......\n", ck.clientId, i, args.Key)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

		if !ok {
			DPrintf("Client[%d] Get request failed!\n", ck.clientId)
			ck.knownLeader = -1 // 没收到回复则下次向其他kvserver发送
			continue
		} else { // 成功收到rpc回复
			switch reply.Err {
			case OK:
				ck.commandNum++
				ck.knownLeader = i // 找到了leader
				DPrintf("Client[%d] request(Seq:%d) for Get(key:%v) [successfully]!\n", ck.clientId, args.CmdNum, args.Key)
				return reply.Value
			case ErrWrongLeader:
				ck.knownLeader = -1
				DPrintf("Client[%d] request(Seq:%d) for Get(key:%v) but [WrongLeader]!\n", ck.clientId, args.CmdNum, args.Key)
				continue
			case ErrNoKey:
				ck.knownLeader = i
				ck.commandNum++
				DPrintf("Client[%d] request(Seq:%d) for Get(key:%v) but [NoKey]!\n", ck.clientId, args.CmdNum, args.Key)
				return "" // key不存在，返回空串
			case ErrTimeout:
				ck.knownLeader = -1 // 下次需要重新找个server发送请求
				DPrintf("Client[%d] request(Seq:%d) for Get(key:%v) but [Timeout]!\n", ck.clientId, args.CmdNum, args.Key)
				continue
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	// You will have to modify this function.

	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		CmdNum:   ck.commandNum,
	}

	i := 0 // 从server 0开始发送请求
	serversNum := len(ck.servers)
	for ; ; i = (i + 1) % serversNum { // 循环依次向每一个server请求直至找到leader

		if ck.knownLeader != -1 { // 如果找到了leader
			i = ck.knownLeader // 下次直接向leader发起请求
		} else {
			time.Sleep(time.Millisecond * 5)
		}

		reply := PutAppendReply{}

		DPrintf("Client[%d] request [%d] for PutAppend(key:%v, value:%v)......\n", ck.clientId, i, args.Key, args.Value)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

		if !ok {
			DPrintf("Client[%d] PutAppend request failed!\n", ck.clientId)
			ck.knownLeader = -1 // 没收到回复则下次向其他kvserver发送
			continue
		} else { // 成功收到rpc回复
			switch reply.Err {
			case OK: // 一切正常
				ck.commandNum++
				ck.knownLeader = i // 找到leader
				DPrintf("Client[%d] request(Seq:%d) for PutAppend [successfully]!\n", ck.clientId, args.CmdNum)
				return
			case ErrWrongLeader: // 连接kvserver不是leader
				ck.knownLeader = -1 // 下次发给reply中携带的leaderId
				DPrintf("Client[%d] request(Seq:%d) for PutAppend but [WrongLeader]!\n", ck.clientId, args.CmdNum)
				continue
			case ErrNoKey:
				ck.commandNum++
				ck.knownLeader = i // 找到leader
				DPrintf("Client[%d] request(Seq:%d) for Append but [NoKey]!\n", ck.clientId, args.CmdNum)
				return // key不存在，append类似put处理
			case ErrTimeout:
				ck.knownLeader = -1 // 下次需要重新找个server发送请求
				DPrintf("Client[%d] request(Seq:%d) for PutAppend but [Timeout]!\n", ck.clientId, args.CmdNum)
				continue
			}
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
