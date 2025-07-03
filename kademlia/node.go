package kademlia

import (
	"crypto/sha1"
	"math/big"

	//采用sha1进行一致性哈希

	"os"

	"github.com/sirupsen/logrus"
)

const m = 160
const k = 20

type KademliaNode struct {
	online bool
}

// Hash Function
func ConsistentHash(addr string) *big.Int {
	h := sha1.New()
	h.Write([]byte(addr))
	hashBytes := h.Sum(nil)
	id := new(big.Int).SetBytes(hashBytes)
	return id
}

func init() {
	f, _ := os.Create("dht-chord-test.log")
	logrus.SetOutput(f)
}

func (node *KademliaNode) ping(addr string) bool {
	//TODO
	return true
}
