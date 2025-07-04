package kademlia

import (
	"sync"
)

type ListNode struct {
	Prev *ListNode
	Next *ListNode
	Addr string
}

// Kademlia中，每个结点维护160个桶
type Bucket struct {
	num        int //当前ListNode的数量
	addr       string
	current    *KademliaNode
	head       *ListNode
	tail       *ListNode
	BucketLock sync.RWMutex
}

func (bucket *Bucket) Initialize(addr string, node *KademliaNode) {
	bucket.BucketLock.Lock()
	bucket.num = 0
	bucket.head = new(ListNode)
	bucket.tail = new(ListNode)
	bucket.addr = addr
	bucket.current = node
	bucket.head.Next = bucket.tail
	bucket.head.Prev = nil
	bucket.head.Addr = ""
	bucket.tail.Prev = bucket.head
	bucket.tail.Next = nil
	bucket.tail.Addr = ""
	bucket.BucketLock.Unlock()
}

func (bucket *Bucket) Size() int {
	bucket.BucketLock.RLock()
	size := bucket.num
	bucket.BucketLock.Unlock()
	return size
}

func (bucket *Bucket) PushTail(addr string) {
	bucket.BucketLock.Lock()
	current := ListNode{bucket.tail.Prev, bucket.tail, addr}
	bucket.tail.Prev.Next = &current
	bucket.tail.Prev = &current
	bucket.num++
	bucket.BucketLock.Unlock()
}

func (bucket *Bucket) MoveToTail(node *ListNode) {
	bucket.BucketLock.Lock()
	if node == bucket.head || node == bucket.tail || node == nil {
		bucket.BucketLock.Unlock()
		return
	}
	if node.Prev == nil || node.Next == nil {
		bucket.BucketLock.Unlock()
		return
	}
	if node.Next == bucket.tail {
		bucket.BucketLock.Unlock()
		return
	}
	node.Prev.Next = node.Next
	node.Next.Prev = node.Prev
	node.Prev = bucket.tail.Prev
	node.Next = bucket.tail
	bucket.tail.Prev.Next = node
	bucket.tail.Prev = node
	bucket.BucketLock.Unlock()
}

func (bucket *Bucket) Find(addr string) *ListNode {
	bucket.BucketLock.RLock()
	current := bucket.head.Next
	for current != bucket.tail {
		if current.Addr == addr {
			bucket.BucketLock.RUnlock()
			return current
		}
		current = current.Next
	}
	bucket.BucketLock.RUnlock()
	return nil
}

func (bucket *Bucket) Delete(current *ListNode) {
	if current == nil || current.Prev == nil || current.Next == nil {
		return
	}
	bucket.BucketLock.Lock()
	current.Prev.Next = current.Next
	current.Next.Prev = current.Prev
	bucket.num--
	bucket.BucketLock.Unlock()
	current.Next = nil
	current.Prev = nil
}

func (bucket *Bucket) Update(addr string, online bool) {
	if bucket.addr == addr {
		return
	}
	current := bucket.Find(addr)
	if !online {
		bucket.Delete(current)
		return
	}
	if current != nil {
		bucket.MoveToTail(current)
		return
	}
	size := bucket.Size()
	if size < k {
		bucket.PushTail(addr)
		return
	}
	bucket.BucketLock.RLock()
	p := bucket.head.Next
	bucket.BucketLock.RUnlock()
	if bucket.current.ping(p.Addr) {
		bucket.MoveToTail(p)
	} else {
		bucket.Delete(p)
		bucket.PushTail(addr)
	}
}

func (bucket *Bucket) All() (nodeList []string) {
	bucket.BucketLock.RLock()
	current := bucket.head.Next
	for current != bucket.tail {
		nodeList = append(nodeList, current.Addr)
		current = current.Next
	}
	bucket.BucketLock.RUnlock()
	return nodeList
}
