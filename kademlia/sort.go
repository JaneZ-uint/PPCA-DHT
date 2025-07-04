package kademlia

import (
	"math/big"
	"sync"
)

type Unit struct {
	prev *Unit
	next *Unit
	addr string
	dis  *big.Int
	call bool
}

type SortList struct {
	target *big.Int
	head   *Unit
	tail   *Unit
	Lock   sync.RWMutex
}

func (sortList *SortList) Initialize(target *big.Int) {
	sortList.Lock.Lock()
	sortList.target = target
	sortList.head = new(Unit)
	sortList.tail = new(Unit)
	sortList.head.next = sortList.tail
	sortList.head.prev = nil
	sortList.tail.prev = sortList.head
	sortList.tail.next = nil
	sortList.Lock.Unlock()
}

// 顺序插入
func (sortList *SortList) Insert(addr string) {
	sortList.Lock.Lock()
	defer sortList.Lock.Unlock()
	dist := new(big.Int).Xor(ConsistentHash(addr), sortList.target)
	current := sortList.head
	if current.next == sortList.tail {
		newUnit := Unit{current, current.next, addr, dist, false}
		current.next.prev = &newUnit
		current.next = &newUnit
		return
	}
	for current.next != sortList.tail {
		if dist.Cmp(current.next.dis) <= 0 {
			newUnit := Unit{current, current.next, addr, dist, false}
			current.next.prev = &newUnit
			current.next = &newUnit
			return
		}
	}
	newUnit := Unit{current, current.next, addr, dist, false}
	current.next.prev = &newUnit
	current.next = &newUnit
}

func (sortList *SortList) GetFirstThree() []string {
	sortList.Lock.Lock()
	defer sortList.Lock.Unlock()
	var list []string
	current := sortList.head.next
	for current != sortList.tail {
		if !current.call {
			list = append(list, current.addr)
			current.call = true
		}
		if len(list) == a {
			break
		}
		current = current.next
	}
	return list
}
