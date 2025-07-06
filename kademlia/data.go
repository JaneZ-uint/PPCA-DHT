package kademlia

import (
	"sync"
)

type Data struct {
	data map[string]string
	Lock sync.RWMutex
}

func (current *Data) Put(tmp KeyValue) {
	current.Lock.Lock()
	defer current.Lock.Unlock()
	current.data[tmp.key] = tmp.value
}

func (current *Data) Get(addr string) BV {
	current.Lock.RLock()
	defer current.Lock.RUnlock()
	value, ok := current.data[addr]
	if !ok {
		return BV{false, ""}
	}
	return BV{true, value}
}
