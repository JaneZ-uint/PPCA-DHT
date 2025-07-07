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
	current.data[tmp.Key] = tmp.Value
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

func (current *Data) GetAll() []KeyValue {
	current.Lock.RLock()
	defer current.Lock.RUnlock()
	var result []KeyValue
	for k, v := range current.data {
		result = append(result, KeyValue{k, v})
	}
	return result
}
