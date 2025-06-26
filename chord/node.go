package chord

import (
	network "dht/network"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"crypto/sha1"
	"math/big"
	//采用sha1进行一致性哈希
)

const m = 160
const n = 10 //后继列表长度

func init() {
	f, _ := os.Create("dht-chord-test.log")
	logrus.SetOutput(f)
}

type Pair struct {
	Key   string
	Value string
}

// Hash Function
func ConsistentHash(addr string) *big.Int {
	h := sha1.New()
	h.Write([]byte(addr))
	hashBytes := h.Sum(nil)
	id := new(big.Int).SetBytes(hashBytes)
	return id
}

// chord 结点
type ChordNode struct {
	Addr   string // address and port number of the node, e.g., "localhost:1234"
	online bool
	ID     *big.Int //After Hashing

	QuitSign chan bool
	//QuitLock sync.RWMutex
	*network.NetworkStation

	//暂时全上锁，后根据需要更改
	predecessor   string
	preLock       sync.RWMutex
	successorList [n + 1]string
	suLock        sync.RWMutex

	data     map[string]string
	dataLock sync.RWMutex
	//备份数据，为应对前驱结点突然崩溃，存储前驱结点的全部信息
	dataBackup     map[string]string
	dataBackupLock sync.RWMutex

	fingerTable [m + 1]string
	fingerLock  sync.RWMutex
}

// 初始化每一个新节点，用于userdef.go中的
func (node *ChordNode) Init(addr string) {
	node.NetworkStation.Init(addr)
	node.Addr = addr
	node.ID = ConsistentHash(addr)
	node.online = false
}

// Some tool functions
// 左开右闭 ( ]
func Contain(Left *big.Int, Right *big.Int, Current *big.Int) bool {
	LRTmp := Left.Cmp(Right)
	CLTmp := Current.Cmp(Left)
	CRTmp := Current.Cmp(Right)
	if LRTmp < 0 {
		if CLTmp > 0 && (CRTmp == 0 || CRTmp < 0) {
			return true
		}
		return false
	} else if LRTmp == 0 {
		return false
	} else {
		if CLTmp < 0 {
			return true
		} else if CRTmp == 0 || CRTmp < 0 {
			return true
		} else {
			return false
		}
	}
}

// 双开区间 ( )
func ContainOpen(Left *big.Int, Right *big.Int, Current *big.Int) bool {
	LRTmp := Left.Cmp(Right)
	CLTmp := Current.Cmp(Left)
	CRTmp := Current.Cmp(Right)
	if LRTmp < 0 {
		if CLTmp > 0 && CRTmp < 0 {
			return true
		}
		return false
	} else if LRTmp == 0 {
		return false
	} else {
		if CLTmp < 0 {
			return true
		} else if CRTmp < 0 {
			return true
		} else {
			return false
		}
	}
}

// 在node n的finger table中寻找identifier k的最近的predecessor
// 没有通信故不为RPC method
func (node *ChordNode) ClosestPrecedingFinger(key *big.Int) string {
	logrus.Infof("Node %s finds its ClosestPrecedingFinger to %v, node.Addr", key)
	for i := m; i >= 1; i-- {
		//注意，这里读fingerTable时要上锁
		node.fingerLock.RLock()
		//TODO 这里是错误的，不应该这么写
		current := node.fingerTable[i]
		node.fingerLock.RUnlock()
		if !node.ping(node.fingerTable[i]) { //check online
			continue
		}
		if ContainOpen(node.ID, key, ConsistentHash(current)) {
			return node.fingerTable[i]
		}
	}
	return node.Addr
}

// stabilize 函数
func (node *ChordNode) Stabilize() {
	var successor string
	node.GetSuccessor("", &successor)
	var successorID *big.Int
	successorID = ConsistentHash(successor)
	var predecessor string
	err := node.RemoteCall(successor, "ChordNode.Predecessor", "", &predecessor)
	if err != nil {
		logrus.Error("Can't find successor due to:", err)
		return
	}
	var predecessorID *big.Int
	predecessorID = ConsistentHash(predecessor)
	if predecessor != "" && predecessor != node.Addr && ContainOpen(node.ID, successorID, predecessorID) {
		//更新后继
		node.suLock.Lock()
		for i := m; i > 0; i-- {
			node.successorList[i] = node.successorList[i-1]
		}
		node.successorList[0] = predecessor
		node.suLock.Unlock()
		//当前结点的后继发生改变，意味着backup Data 发生了改变
		// node np ns  需要修改 np ns 的backupData
		//TODO!
	}
	err1 := node.RemoteCall(successor, "ChordNode.Notify", node.Addr, nil)
	if err1 != nil {
		logrus.Error("Failed Notify:", err1)
	}
}

// notify 函数 对ns调用 传入插入节点n ip
func (node *ChordNode) Notify(target string, reply *struct{}) error {
	var predecessor string
	node.GetPredecessor("", &predecessor)
	predecessorID := ConsistentHash(predecessor)
	if predecessor == "" || ContainOpen(predecessorID, ConsistentHash(target), node.ID) {
		node.preLock.Lock()
		node.predecessor = target
		node.preLock.Unlock()
	}
	return nil
}

// fix_finger 函数 更新fingerTable
func (node *ChordNode) fix_finger() {

}

// maintain 函数
func (node *ChordNode) maintain() {
	logrus.Infof("Node %s starts to maintain", node.Addr)
	//开2个线程，定期进行stabilize 和 fix finger操作
	go func() {
		for node.online {
			node.Stabilize()
			time.Sleep(50 * time.Millisecond)
		}
	}()
	go func() {
		for node.online {
			node.fix_finger()
			time.Sleep(50 * time.Millisecond)
		}
	}()
}

// Client  活体检测
func (node *ChordNode) ping(addr string) bool {
	err := node.RemoteCall(addr, "ChordNode.Ping", "", nil)
	if err != nil {
		return false
	}
	return true
}

// RPC 服务端 Methods  符合RPC规则
func (node *ChordNode) Ping(_ string, _ *struct{}) error {
	if node.online {
		return nil
	}
	return errors.New("offline")
}

func (node *ChordNode) GetSuccessor(_ string, reply *string) error {
	logrus.Infof("Node %s gets its successor", node.Addr)
	node.suLock.RLock()
	for i := 0; i <= n; i++ {
		if err := node.RemoteCall(node.successorList[i], "ChordNode.Ping", "", nil); err == nil {
			*reply = node.successorList[i]
			node.suLock.RUnlock()
			return nil
		}
	}
	node.suLock.RUnlock()
	return errors.New("No successor")
}

func (node *ChordNode) GetPredecessor(_ string, reply *string) error {
	logrus.Infof("Node %s gets its Predecessor", node.Addr)
	node.preLock.RLock()
	*reply = node.predecessor
	node.preLock.RUnlock()
	return nil
}

func (node *ChordNode) GetSuccessorList(_ string, reply *[n + 1]string) error {
	logrus.Infof("Node %s gets its successor_list", node.Addr)
	node.suLock.RLock()
	*reply = node.successorList
	node.suLock.RUnlock()
	return nil
}

func (node *ChordNode) FindSuccessor(key *big.Int, reply *string) error {
	logrus.Infof("Find successor of %v from node %s with %v", key, node.Addr, node.ID)
	if key.Cmp(node.ID) == 0 {
		*reply = node.Addr
		return nil
	}
	err1 := node.FindPredecessor(key, reply)
	if err1 != nil {
		logrus.Error("Find predecessor error: ", err1)
		return err1
	}
	err2 := node.RemoteCall(*reply, "ChordNode.GetSuccessor", "", reply)
	if err2 != nil {
		return err2
	}
	return nil
}

func (node *ChordNode) FindPredecessor(key *big.Int, reply *string) error {
	*reply = node.Addr
	var successor string
	var successorID *big.Int
	node.GetSuccessor("", &successor)
	successorID = ConsistentHash(successor)
	if !Contain(node.ID, successorID, key) {
		var closestPrecedingFinger string
		closestPrecedingFinger = node.ClosestPrecedingFinger(key)
		err := node.RemoteCall(closestPrecedingFinger, "ChordNode.FindPredecessor", key, reply)
		if err != nil {
			return err
		}
	}
	return nil
}

//DHT Method
//DHT 的标准接口
//开始时调用

func (node *ChordNode) Run(wg *sync.WaitGroup) {
	node.QuitSign = make(chan bool)
	node.online = true
	node.preLock.Lock()
	node.predecessor = ""
	node.preLock.Unlock()
	node.suLock.Lock()
	for i := 0; i <= m; i++ {
		node.successorList[i] = node.Addr
	}
	node.suLock.Unlock()
	node.dataLock.Lock()
	node.data = make(map[string]string)
	node.dataLock.Unlock()
	node.dataBackupLock.Lock()
	node.dataBackup = make(map[string]string)
	node.dataBackupLock.Unlock()
	go node.NetworkStation.Run(node, wg)
}

// 创建chord 中第一个结点 Create
// Create a new network.
func (node *ChordNode) Create() {
	logrus.Info("Create")
	node.preLock.Lock()
	node.predecessor = node.Addr
	node.preLock.Unlock()
	<-node.QuitSign
	node.maintain()
}

// 加入一个新的结点 Join 接口
func (node *ChordNode) Join(addr string) bool {
	logrus.Infof("Node %s join chord", node.Addr)
	err1 := node.RemoteCall(addr, "ChordNode.Ping", "", nil)
	if err1 != nil {
		logrus.Error("Node offline", err1)
		return false
	}
	var successor string
	err2 := node.RemoteCall(addr, "ChordNode.FindSuccessor", "", &successor)
	if err2 != nil {
		logrus.Error("Get Successor Failed", err2)
		return false
	}
	node.suLock.Lock()
	node.successorList[0] = successor
	node.suLock.Unlock()
	node.preLock.Lock()
	node.predecessor = ""
	node.preLock.Unlock()
	node.maintain() //Main task
	return true
}

//Put a key-value pair into the network (if key exists, update the value).
// Return "true" if success, "false" otherwise.
//func (node *ChordNode) Put(key string, value string) bool{

//}

// Get a key-value pair from the network.
// Return "true" and the value if success, "false" otherwise.
//func (node *ChordNode) Get(key string) (bool, string) {

//}

// Remove a key-value pair identified by KEY from the network.
// Return "true" if success, "false" otherwise.
//func (node *ChordNode) Delete(key string) bool {

//}

//几个和数据库处理相关的函数 RPC Method
//删除成功reply true 否则false
//func (node *ChordNode) DeleteDataBackup(key []string,reply *bool) error{

//}

//func (node *ChordNode) DeleteData(key []string,reply *bool) error{

//}
