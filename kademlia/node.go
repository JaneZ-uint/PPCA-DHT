package kademlia

import (
	"crypto/sha1"
	network "dht/network"
	"errors"
	"math/big"
	"sync"

	//采用sha1进行一致性哈希

	"os"

	"github.com/sirupsen/logrus"
)

const m = 160
const k = 20

var number [m + 1]*big.Int

type KademliaNode struct {
	online bool
	network.NetworkStation
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

func (node *KademliaNode) FindNode() {

}
