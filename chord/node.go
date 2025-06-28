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

	"math/rand"
)

const m = 160
const n = 10 //后继列表长度

func init() {
	//设置随机数种子
	rand.Seed(time.Now().UnixNano())
	f, _ := os.Create("dht-chord-test.log")
	logrus.SetOutput(f)
}

type Pair struct {
	Key   string
	Value string
}

type PS struct {
	Pre string
	Suc string
}

type BV struct {
	IsGet bool
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

// Caculate 计算fingerTable
func Caculate(key *big.Int, i int) *big.Int {
	powerOfTwo := new(big.Int).Lsh(big.NewInt(1), uint(i-1))
	sum := new(big.Int).Add(key, powerOfTwo)
	modulus := new(big.Int).Lsh(big.NewInt(1), uint(m))
	result := new(big.Int).Mod(sum, modulus)
	return result
}

// 在node n的finger table中寻找identifier k的最近的predecessor
// 没有通信故不为RPC method
func (node *ChordNode) ClosestPrecedingFinger(key *big.Int) string {
	logrus.Infof("[ClosestPrecedingFinger] Node %s finds its ClosestPrecedingFinger to %v, node.Addr", key)
	for i := m; i >= 1; i-- {
		//注意，这里读fingerTable时要上锁
		node.fingerLock.RLock()
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
		logrus.Error("[Stabilize] Can't find Predecessor:", err)
		return
	}
	var predecessorID *big.Int
	predecessorID = ConsistentHash(predecessor)
	// n np ns
	if predecessor != "" && predecessor != node.Addr && ContainOpen(node.ID, predecessorID, successorID) {
		//更新后继
		node.suLock.Lock()
		for i := m; i > 0; i-- {
			node.successorList[i] = node.successorList[i-1]
		}
		node.successorList[0] = predecessor
		node.suLock.Unlock()
	}
	err1 := node.RemoteCall(successor, "ChordNode.Notify", node.Addr, nil)
	if err1 != nil {
		logrus.Error("[Stabilize] Failed Notify:", err1)
	}
}

// notify 函数 对ns调用 传入插入节点n ip
// np n ns
func (node *ChordNode) Notify(target string, reply *struct{}) error {
	var predecessor string
	node.GetPredecessor("", &predecessor)
	predecessorID := ConsistentHash(predecessor)
	// np n ns
	if predecessor == "" || ContainOpen(predecessorID, ConsistentHash(target), node.ID) {
		node.preLock.Lock()
		node.predecessor = target
		node.preLock.Unlock()
		//更新backup
		err := node.RemoteCall(target, "ChordNode.GetDataForBackup", "", &node.dataBackup)
		if err != nil {
			logrus.Error("[Notify] failed when updating backup:", err)
			return err
		}
	}
	return nil
}

// fix_finger 函数 更新fingerTable
func (node *ChordNode) fix_finger() {
	i := rand.Intn(159) + 2
	var successor string
	err := node.FindSuccessor(Caculate(node.ID, i), &successor)
	if err != nil {
		logrus.Error("[fix_finger] Fix finger table failed:", err)
		return
	}
	node.fingerLock.Lock()
	node.fingerTable[i] = successor
	node.fingerLock.Unlock()
	logrus.Infof("[fix_finger] Node %s", node.Addr)
}

// maintain 函数
func (node *ChordNode) maintain() {
	logrus.Infof("[maintain] Node %s starts to maintain", node.Addr)
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
		logrus.Error("[ping] failed:", err)
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

func (node *ChordNode) UpdateSuccessorList() {
	logrus.Infof("[UpdateSuccessorList] Node %s", node.Addr)
	var tmp [n + 1]string
	node.GetSuccessorList("", &tmp)
	for _, ip := range tmp {
		if node.ping(ip) {
			//第一个活的node
			var info [n + 1]string
			err := node.RemoteCall(ip, "ChordNode.GetSuccessorList", "", &info)
			if err != nil {
				logrus.Error("[UpdateSuccessorList] failed to get SuccessorList:", err)
				continue
			}
			node.suLock.Lock()
			for j := 1; j <= n; j++ {
				node.successorList[j] = info[j-1]
			}
			node.successorList[0] = ip
			node.suLock.Unlock()
			return
		}
	}
}

func (node *ChordNode) GetSuccessor(_ string, reply *string) error {
	logrus.Infof("[GetSuccessor] Node %s gets its successor", node.Addr)
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
	logrus.Infof("[GetPredecessor] Node %s gets its Predecessor", node.Addr)
	node.preLock.RLock()
	*reply = node.predecessor
	node.preLock.RUnlock()
	return nil
}

func (node *ChordNode) GetSuccessorList(_ string, reply *[n + 1]string) error {
	logrus.Infof("[GetSuccessorList] Node %s gets its successor_list", node.Addr)
	node.suLock.RLock()
	*reply = node.successorList
	node.suLock.RUnlock()
	return nil
}

// 注意，这里找的是key的Successor，而不是Node
// 二者的successor的定义存在差异
func (node *ChordNode) FindSuccessor(key *big.Int, reply *string) error {
	logrus.Infof("[Find successor] of %v from node %s with %v", key, node.Addr, node.ID)
	if key.Cmp(node.ID) == 0 {
		*reply = node.Addr
		return nil
	}
	err1 := node.FindPredecessor(key, reply)
	if err1 != nil {
		logrus.Error("[FindSuccessor] Find predecessor error: ", err1)
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
			logrus.Error("[FindPredecessor] failed:", err)
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
	logrus.Infoln("[Create] Node", node.Addr)
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
		logrus.Error("[Join] Node offline", err1)
		return false
	}
	var successor string
	err2 := node.RemoteCall(addr, "ChordNode.FindSuccessor", "", &successor)
	if err2 != nil {
		logrus.Error("[Join] Get Successor Failed", err2)
		return false
	}
	node.suLock.Lock()
	node.successorList[0] = successor
	node.suLock.Unlock()
	//显然这里缺东西了，fingerTable和后继列表都没有初始化
	node.preLock.Lock()
	node.predecessor = ""
	node.preLock.Unlock()
	node.maintain() //双链表维护
	return true
}

// Put a key-value pair into the network (if key exists, update the value).
// Return "true" if success, "false" otherwise.
func (node *ChordNode) Put(key string, value string) bool {
	keyID := ConsistentHash(key)
	var successor string
	err := node.FindSuccessor(keyID, &successor)
	if err != nil {
		logrus.Error("[Put] failed when finding successor:", err)
		return false
	}
	var data []Pair
	data = append(data, Pair{key, value})
	err1 := node.RemoteCall(successor, "ChordNode.UpdateNode", data, nil)
	if err1 != nil {
		logrus.Error("[Put] Update node failed:", err1)
		return false
	}
	return true
}

// Get a key-value pair from the network.
// Return "true" and the value if success, "false" otherwise.
func (node *ChordNode) Get(key string) (bool, string) {
	keyID := ConsistentHash(key)
	var successor string
	err := node.FindSuccessor(keyID, &successor)
	if err != nil {
		logrus.Error("[Get] failed when finding successor:", err)
		return false, ""
	}
	var target BV
	err1 := node.RemoteCall(successor, "ChordNode.GetValue", key, &target)
	if err1 != nil {
		logrus.Error("[Get] Get value failed:", err1)
		return false, ""
	}
	return target.IsGet, target.Value
}

// Remove a key-value pair identified by KEY from the network.
// Return "true" if success, "false" otherwise.
func (node *ChordNode) Delete(key string) bool {
	keyID := ConsistentHash(key)
	var successor string
	err := node.FindSuccessor(keyID, &successor)
	if err != nil {
		logrus.Error("Delete failed when finding successor:", err)
		return false
	}
	var reply bool
	err1 := node.RemoteCall(successor, "ChordNode.DeleteNode", key, &reply)
	if err1 != nil {
		logrus.Error("[Delete] failed:", err1)
		return false
	}
	if reply == true {
		return true
	}
	return false
}

// "Normally" quit from current network.
// You can inform other nodes in the network that you are leaving.
// "Quit" will not be called before "Create" or "Join".
// For a dhtNode, "Quit" may be called for many times.
// For a quited node, call "Quit" again should have no effect.
func (node *ChordNode) Quit() {
	logrus.Infof("[Quit] Node %s start quit", node.Addr)
	if !node.online {
		logrus.Error("[Quit] Node already quit", node.Addr)
		return
	}
	node.online = false
	var predecessor string
	node.GetPredecessor("", &predecessor)
	node.UpdateSuccessorList()

}

// Quit the network without informing other nodes.
// "ForceQuit" will be checked by TA manually.
func (node *ChordNode) ForceQuit() {
	if node.online {
		node.online = false
		logrus.Info("[ForceQuit]")
		node.StopRPCServer()
	}
}

// 几个和数据库处理相关的函数 RPC Method
//
// 数据清除
func (node *ChordNode) DeleteDataBackup(key []string, reply *bool) error {
	*reply = true
	node.dataBackupLock.Lock()
	for i := range key {
		_, ok := node.data[key[i]]
		if !ok {
			*reply = false
		}
		delete(node.dataBackup, key[i])
	}
	node.dataBackupLock.Unlock()
	return nil
}

func (node *ChordNode) DeleteData(key []string, reply *bool) error {
	*reply = true
	node.dataLock.Lock()
	for i := range key {
		_, ok := node.dataBackup[key[i]]
		if !ok {
			*reply = false
		}
		delete(node.data, key[i])
	}
	node.dataLock.Unlock()
	return nil
}

func (node *ChordNode) DeleteNode(key []string, reply *bool) error {
	var isData bool
	node.DeleteData(key, &isData)
	var successor string
	err := node.GetSuccessor("", &successor)
	if err != nil {
		logrus.Error("[DeleteNode] Get successor Failed:", err)
		return err
	}
	var isBackup bool
	err1 := node.RemoteCall(successor, "ChordNode.DeleteDataBackup", key, &isBackup)
	if err1 != nil {
		logrus.Error("[DeleteNode] Delete successor's backup data failed:", err1)
		return err1
	}
	if isBackup && isData {
		*reply = true
	} else {
		*reply = false
	}
	return nil
}

// 更新数据的函数（Actually 是对称的）
func (node *ChordNode) UpdateData(data []Pair, reply *struct{}) error {
	node.dataLock.Lock()
	for _, value := range data {
		node.data[value.Key] = value.Value
	}
	node.dataLock.Unlock()
	return nil
}

func (node *ChordNode) UpdateBackup(backup []Pair, reply *struct{}) error {
	node.dataBackupLock.Lock()
	for _, value := range backup {
		node.dataBackup[value.Key] = value.Value
	}
	node.dataBackupLock.Unlock()
	return nil
}

func (node *ChordNode) UpdateNode(info []Pair, reply *struct{}) error {
	node.UpdateData(info, nil)
	var successor string
	node.GetSuccessor("", &successor)
	err := node.RemoteCall(successor, "ChordNode.UpdateBackup", info, nil)
	if err != nil {
		logrus.Error("[UpdateNode] Update backup failed:", err)
		return err
	}
	return nil
}

// 从数据库中得到某个值
func (node *ChordNode) GetValue(key string, reply *BV) error {
	node.dataLock.RLock()
	value, ok := node.data[key]
	node.dataLock.RUnlock()
	if !ok {
		reply.IsGet = false
		reply.Value = ""
	} else {
		reply.IsGet = true
		reply.Value = value
	}

	return nil
}

// 获得某个结点的data 用于backup的更新
func (node *ChordNode) GetDataForBackup(_ string, backup *(map[string]string)) error {
	*backup = make(map[string]string)
	node.dataBackupLock.RLock()
	for k, v := range node.data {
		(*backup)[k] = v
	}
	node.dataBackupLock.RUnlock()
	return nil
}

// Some functions for Quit
// np n ns n失效后，需要将自身的backup数据移交给第一个存活后继的backup中
func (node *ChordNode) BackupAddData(_ string, reply *struct{}) error {
	var newInfo []Pair
	node.dataBackupLock.RLock()
	for k, v := range node.dataBackup {
		newInfo = append(newInfo, Pair{k, v})
	}
	node.dataBackupLock.RUnlock()
	node.dataBackupLock.Lock()
	node.dataBackup = make(map[string]string)
	node.dataBackupLock.Unlock()
	err := node.UpdateNode(newInfo, nil)
	if err != nil {
		logrus.Error("[BackupAddData] Update Node Failed:", err)
		return err
	}
	return nil
}

// 将node的predecessor修改为target
func (node *ChordNode) UpdatePredecessor(target string, reply *struct{}) error {
	node.preLock.Lock()
	node.predecessor = target
	node.preLock.Unlock()
	return nil
}
