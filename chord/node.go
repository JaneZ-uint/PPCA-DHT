package chord

import(
	"net"
	"os"
	"net/rpc"
	"sync"
	"time"
	"github.com/sirupsen/logrus"

	"math/big"
	"crypto/sha1"
	//采用sha1进行一致性哈希
)

const m = 160

func init(){
	f, _ := os.Create("dht-chord-test.log")
	logrus.SetOutput(f)
}

type Pair struct{
	Key   string
	Value string
}

//Hash Function
func ConsistentHash(addr string) *big.Int{
	h := sha1.New()
	h.Write([]byte(addr))
	hashBytes := h.Sum(nil)
	id := new(big.Int).SetBytes(hashBytes)
	return id
}

//chord 结点
type ChordNode struct{
	Addr   			string // address and port number of the node, e.g., "localhost:1234"
	online      	bool
	listener  		net.Listener
	server    		*rpc.Server

	ID				*big.Int //After Hashing

	//暂时全上锁，后根据需要更改
	predecessor 	string
	preLock         sync.RWMutex
	successorList   [m + 1]string
    suLock          sync.RWMutex

	data      		map[string]string
	dataLock  		sync.RWMutex
	//备份数据，为应对前驱结点突然崩溃
	dataBackup   	map[string]string
	dataBackupLock  sync.RWMutex

	fingerTable     [m + 1]string
	fingerLock      sync.RWMutex
}

func (node *ChordNode) Init(addr string){
	node.Addr = addr
	node.data = make(map[string]string)
	node.dataBackup = make(map[string]string)
	node.ID = ConsistentHash(addr)
	node.online = false
}

func (node *ChordNode) RunRPCServer(wg *sync.WaitGroup) {
	node.server = rpc.NewServer()
	node.server.Register(node)
	var err error
	node.listener, err = net.Listen("tcp", node.Addr)
	wg.Done()
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	for node.online {
		conn, err := node.listener.Accept()
		if err != nil {
			logrus.Error("accept error: ", err)
			return
		}
		go node.server.ServeConn(conn)
	}
}

func (node *ChordNode) StopRPCServer() {
	node.online = false
	node.listener.Close()
}

//Some tool functions
//左开右闭 ( ]
func Contain(left *big.Int, Right *big.Int, Current *big.Int) bool{
	LRTmp := Left.Cmp(Right)
	CLTmp := Current.Cmp(Left)
	CRTmp := Current.Cmp(Right)
	if LRTmp < 0 {
		if CLTmp > 0 && (CRTmp == 0 || CRTmp < 0) {
			return true
		}
		return false
	}else if LRTmp == 0 {
		return false
	}else{
		if CLTmp < 0 {
			return true
		}else if CRTmp == 0 || CRTmp < 0 {
			return true
		}else{
			return false
		}
	}
}

//双开区间 ( )
func ContainOpen(left *big.Int, Right *big.Int, Current *big.Int) bool{
	LRTmp := Left.Cmp(Right)
	CLTmp := Current.Cmp(Left)
	CRTmp := Current.Cmp(Right)
	if LRTmp < 0 {
		if CLTmp > 0 &&  CRTmp < 0 {
			return true
		}
		return false
	}else if LRTmp == 0 {
		return false
	}else{
		if CLTmp < 0 {
			return true
		}else if  CRTmp < 0 {
			return true
		}else{
			return false
		}
	}
}

//RemoteCall
//是否建立 connection pool 待定
func (node *ChordNode) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "ChordNode.Ping" {
		logrus.Infof("[%s] RemoteCall %s %s %v", node.Addr, addr, method, args)
	}
	// Note: Here we use DialTimeout to set a timeout of 10 seconds.
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		logrus.Error("dialing: ", err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Error("RemoteCall error: ", err)
		return err
	}
	return nil
}

//检查结点是否在线
func (node *ChordNode) checkOnline(addr string) bool{
	client,err := GetClient(addr)
	if err != nil {
		logrus.Infof("Node %s failed check online",addr)
		return false
	}
	if client != nil {
		defer client.Close()
	}else{
		logrus.Infof("Node %s failed check online",addr)
		return false
	}
	logrus.Infof("Node %s check online",addr)
	return true
}

//在node n的finger table中寻找identifier k的最近的predecessor 
// 没有通信故不为RPC method
func (node *ChordNode) ClosestPrecedingFinger(key *big.Int) string{
	logrus.Infof("Node %s finds its ClosestPrecedingFinger to %v, node.Addr",key)
	for i := m; i >= 1 ; i -- {
		//注意，这里读fingerTable时要上锁
		node.fingerLock.RLock
		currentID := node.fingerTable[i]
		node.fingerLock.RUnlock
		if !node.checkOnline(node.fingerTable[i]) {
			continue
		}
		if ContainOpen(node.ID, key, ConsistentHash(currentID)) {
			return fingerTable[i]
		}
	}
	return node.addr
}

//RPC Methods  符合RPC规则
func (node *ChordNode) Ping(_ string, _ *struct{}) error {
	return nil
}

func (node *ChordNode) GetSuccessor(_ string, reply *string) error{
	logrus.Infof("Node %s gets its successor, node.Addr")
	node.suLock.RLock
	*reply = node.successorList[0]
	node.suLock.RUnlock
	return nil
}

func (node *ChordNode) FindSuccessor(key *big.Int, reply *string) error{
	logrus.Infof("Find successor of %v from node %s with %v",key,node.Addr,node.ID)
	if key.Cmp(node.ID) == 0 {
		reply = node.Addr
		return nil
	}
	err1 := FindPredecessor(key, reply)
	if err1 != nil {
		logrus.Error("Find predecessor error: ",err1)
		return err1
	}
	err2 := RemoteCall(*reply,"ChordNode.GetSuccessor","",reply)
	if err2 != nil {
		return err2
	}
	return nil
}

func (node *ChordNode) FindPredecessor(key *big.Int, reply *string) error{
	*reply = node.Addr
	var successor string
	var successorID *big.Int
	node.GetSuccessor("", &successor)
	successorID = ConsistentHash(successor)
	if !Contain(node.ID, successorID , key) {
		var closestPrecedingFinger string
		closestPrecedingFinger = node.ClosestPrecedingFinger(key)
		err := RemoteCall(closestPrecedingFinger,"ChordNode.FindPredecessor",key,reply)
		if err != nil {
			return err
		}
	}
	return nil
}