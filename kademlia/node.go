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
func (node *KademliaNode) FindNode(key *big.Int) (kElem []string) {
	index := GetIndex(node.ID, key)
	var list []string
	if index != -1 {
		list = node.BucketList[index].All()
		kElem = append(kElem, list...)
	}
	if len(kElem) == k {
		return kElem
	}
	for i := index + 1; i <= m; i++ {
		list = node.BucketList[i].All()
		if len(kElem)+len(list) < k {
			kElem = append(kElem, list...)
		} else if len(kElem)+len(list) == k {
			kElem = append(kElem, list...)
			break
		} else {
			for j := 0; j < len(list); j++ {
				kElem = append(kElem, list[j])
				if len(kElem) == k {
					break
				}
			}
			break
		}
	}
	if len(kElem) == k {
		return kElem
	}
	for i := index - 1; i >= 0; i-- {
		list = node.BucketList[i].All()
		if len(kElem)+len(list) < k {
			kElem = append(kElem, list...)
		} else if len(kElem)+len(list) == k {
			kElem = append(kElem, list...)
			break
		} else {
			for j := 0; j < len(list); j++ {
				kElem = append(kElem, list[j])
				if len(kElem) == k {
					break
				}
			}
			break
		}
	}
	return kElem
}

// 返回整个系统中距离key最近的k个结点
// 对FindNode的递归调用
func (node *KademliaNode) Lookup(key *big.Int) (kElem []string) {

}
