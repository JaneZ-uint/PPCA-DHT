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
}

type SortList struct {
	target   *big.Int
	head     *Unit
	tail     *Unit
	Lock     sync.RWMutex
	num      int
	visitmap map[string]bool
	callmap  map[string]bool
}

func (sortList *SortList) Initialize(target *big.Int, addr string) {
	sortList.Lock.Lock()
	sortList.target = target
	sortList.head = new(Unit)
	sortList.tail = new(Unit)
	sortList.head.next = sortList.tail
	sortList.head.prev = nil
	sortList.tail.prev = sortList.head
	sortList.tail.next = nil
	sortList.num = 0
	sortList.visitmap = make(map[string]bool)
	sortList.callmap = make(map[string]bool)
	sortList.callmap[addr] = true
	sortList.visitmap[addr] = true
	sortList.Lock.Unlock()
}

// 顺序插入
func (sortList *SortList) Insert(addr string) bool {
	sortList.Lock.Lock()
	defer sortList.Lock.Unlock()
	if sortList.visitmap[addr] {
		return false
	}
	sortList.visitmap[addr] = true
	dist := new(big.Int).Xor(ConsistentHash(addr), sortList.target)
	current := sortList.head
	sortList.num++
	if current.next == sortList.tail {
		newUnit := Unit{current, current.next, addr, dist}
		current.next.prev = &newUnit
		current.next = &newUnit
		return true
	}
	for current.next != sortList.tail {
		if dist.Cmp(current.next.dis) < 0 {
			newUnit := Unit{current, current.next, addr, dist}
			current.next.prev = &newUnit
			current.next = &newUnit
			return true
		}
		current = current.next
	}
	newUnit := Unit{current, current.next, addr, dist}
	current.next.prev = &newUnit
	current.next = &newUnit
	return true
}

// 前a个未被请求过的结点
func (sortList *SortList) GetFirstThree() []string {
	sortList.Lock.Lock()
	defer sortList.Lock.Unlock()
	var list []string
	current := sortList.head.next
	for current != sortList.tail {
		if !sortList.callmap[current.addr] {
			list = append(list, current.addr)
			sortList.callmap[current.addr] = true
		}
		if len(list) == a {
			break
		}
		current = current.next
	}
	return list
}

// 所有未被请求过的结点
func (sortList *SortList) GetAllUncall() []string {
	var list []string
	sortList.Lock.RLock()
	defer sortList.Lock.RUnlock()
	current := sortList.head.next
	for current != sortList.tail {
		if !sortList.callmap[current.addr] {
			list = append(list, current.addr)
		}
		current = current.next
	}
	return list
}

// 删除某个指定ip的结点
func (sortList *SortList) Delete(addr string) {
	sortList.Lock.Lock()
	defer sortList.Lock.Unlock()
	current := sortList.head.next
	for current != sortList.tail {
		if current.addr == addr {
			sortList.num--
			current.prev.next = current.next
			current.next.prev = current.prev
			current.prev = nil
			current.next = nil
			break
		}
		current = current.next
	}
}

func (sortList *SortList) IsEmpty() bool {
	sortList.Lock.RLock()
	defer sortList.Lock.RUnlock()
	return sortList.num == 0
}

// 返回最小距离
func (sortList *SortList) GetFront() string {
	sortList.Lock.RLock()
	defer sortList.Lock.RUnlock()
	if sortList.IsEmpty() {
		return ""
	} else {
		return sortList.head.next.addr
	}
}

// 返回k个最近的
func (sortList *SortList) GetFirstK() []string {
	sortList.Lock.Lock()
	defer sortList.Lock.Unlock()
	var list []string
	current := sortList.head.next
	for current != sortList.tail {
		if !sortList.callmap[current.addr] {
			list = append(list, current.addr)
			sortList.callmap[current.addr] = true
		}
		if len(list) == k {
			break
		}
		current = current.next
	}
	return list
}
