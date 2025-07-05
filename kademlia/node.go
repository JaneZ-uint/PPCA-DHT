package kademlia

import (
	"crypto/sha1"
	network "dht/network"
	"errors"
	"math/big"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

const m = 160
const k = 20
const a = 3

var number [m + 1]*big.Int

type KademliaNode struct {
	online bool
	network.NetworkStation
	BucketList [m + 1]Bucket
	Addr       string
	ID         *big.Int
	data       map[string]string
	QuitLock   sync.RWMutex
	wg         sync.WaitGroup
}

// Hash Function
func ConsistentHash(addr string) *big.Int {
	h := sha1.New()
	h.Write([]byte(addr))
	hashBytes := h.Sum(nil)
	id := new(big.Int).SetBytes(hashBytes)
	return id
}

// Initialize
func InitializeNumber() {
	base := big.NewInt(2)
	number[0] = big.NewInt(1)
	for i := 1; i <= m; i++ {
		number[i] = new(big.Int)
		number[i].Mul(number[i-1], base)
	}
}

// Get Index Function
func GetIndex(current *big.Int, key *big.Int) int {
	dis := new(big.Int).Xor(current, key)
	for i := m; i >= 0; i-- {
		if dis.Cmp(number[i]) >= 0 {
			return i
		}
	}
	return -1
}

func init() {
	InitializeNumber()
	f, _ := os.Create("dht-chord-test.log")
	logrus.SetOutput(f)
}

func (node *KademliaNode) Init(addr string) {
	node.Addr = addr
	node.ID = ConsistentHash(addr)
	node.online = false
}

func (node *KademliaNode) ping(addr string) bool {
	const maxRetries = 1
	var err error
	for i := 0; i <= maxRetries; i++ {
		if err = node.RemoteCall(addr, "chord.Ping", "", nil); err == nil {
			return true
		}
	}
	return false
}

func (node *KademliaNode) Ping(_ string, reply *struct{}) error {
	if node.online {
		return nil
	}
	return errors.New("offline")
}

// k-bucket 的更新
func (node *KademliaNode) update(addr string, online bool) {
	index := GetIndex(node.ID, ConsistentHash(addr))
	if index != -1 {
		node.BucketList[index].Update(addr, online)
	}
}

// "Run" is called after calling "NewNode". You can do some initialization works here.
func (node *KademliaNode) Run(waitgroup *sync.WaitGroup) {
	node.online = true
	node.InitRPC(node, "kademlia")
	go node.RunRPCServer(node.Addr, waitgroup)
}

// "Create" or "Join" will be called after calling "Run".
// For a dhtNode, either "Create" or "Join" will be called, but not both.

// Create a new network.
func (node *KademliaNode) Create() {

}

// Join an existing network. Return "true" if join succeeded and "false" if not.
func (node *KademliaNode) Join(addr string) bool {

}

// "Normally" quit from current network.
// You can inform other nodes in the network that you are leaving.
// "Quit" will not be called before "Create" or "Join".
// For a dhtNode, "Quit" may be called for many times.
// For a quited node, call "Quit" again should have no effect.
func (node *KademliaNode) Quit() {

}

// Quit the network without informing other nodes.
// "ForceQuit" will be checked by TA manually.
func (node *KademliaNode) ForceQuit() {
	if node.online {
		node.online = false
		logrus.Info("[ForceQuit]")
		node.StopRPCServer()
	}
}

// Check whether the node identified by addr is in the network.
// Ping(addr string) bool

// Put a key-value pair into the network (if key exists, update the value).
// Return "true" if success, "false" otherwise.
func (node *KademliaNode) Put(key string, value string) bool {

}

// Get a key-value pair from the network.
// Return "true" and the value if success, "false" otherwise.
func (node *KademliaNode) Get(key string) (bool, string) {

}

// Remove a key-value pair identified by KEY from the network.
// Return "true" if success, "false" otherwise.
func (node *KademliaNode) Delete(key string) bool {

}

// 返回当前结点知道的距离key最近的k个结点
// 可能需要修改，再议吧...
// upd:的确要改（）—— 2025.7.5.1:26
// upd:并不用改 —— 2025.7.5.1:36
func (node *KademliaNode) FindNode(key *big.Int, kElem *[]string) error {
	index := GetIndex(node.ID, key)
	var list []string
	if index != -1 {
		list = node.BucketList[index].All()
		*kElem = append(*kElem, list...)
	}
	if len(*kElem) == k {
		return nil
	}
	for i := index + 1; i <= m; i++ {
		list = node.BucketList[i].All()
		if len(*kElem)+len(list) < k {
			*kElem = append(*kElem, list...)
		} else if len(*kElem)+len(list) == k {
			*kElem = append(*kElem, list...)
			break
		} else {
			for j := 0; j < len(list); j++ {
				*kElem = append(*kElem, list[j])
				if len(*kElem) == k {
					break
				}
			}
			break
		}
	}
	if len(*kElem) == k {
		return nil
	}
	for i := index - 1; i >= 0; i-- {
		list = node.BucketList[i].All()
		if len(*kElem)+len(list) < k {
			*kElem = append(*kElem, list...)
		} else if len(*kElem)+len(list) == k {
			*kElem = append(*kElem, list...)
			break
		} else {
			for j := 0; j < len(list); j++ {
				*kElem = append(*kElem, list[j])
				if len(*kElem) == k {
					break
				}
			}
			break
		}
	}
	return nil
}

func (node *KademliaNode) CallConcurrency(callList []string, sequence *SortList, key *big.Int) {
	var wg sync.WaitGroup
	for i := range callList {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			var list []string
			err := node.RemoteCall(addr, "kademlia.FindNode", key, &list)
			if err != nil {
				//addr失活 线程可以直接杀死了
				node.update(addr, false)
				sequence.Delete(addr)
				return
			}
			//成功则upd
			node.update(addr, true)
			for j := range list {
				sequence.Insert(list[j])
			}
		}(callList[i])
	}
	wg.Wait()
}

// 返回整个系统中距离key最近的k个结点
// 对FindNode的递归调用
func (node *KademliaNode) Lookup(key *big.Int) (kElem []string) {
	Sequence := SortList{}
	Sequence.Initialize(key)
	var firstKElem []string
	node.FindNode(key, &firstKElem)
	for i := range firstKElem {
		Sequence.Insert(firstKElem[i])
	}
	for {
		var callList []string
		callList = Sequence.GetFirstThree()
		closest := callList[0] //最近的
		//请求这些结点执行FindNode
		node.CallConcurrency(callList, &Sequence, key)
		//这里之所以可以这么写和sort.go里的插入策略有关
		if Sequence.IsEmpty() || Sequence.GetFront() != closest {
			callList = Sequence.GetAllUncall()
			node.CallConcurrency(callList, &Sequence, key)
			kElem = Sequence.GetFirstK()
			break
		}
	}
	return kElem
}
