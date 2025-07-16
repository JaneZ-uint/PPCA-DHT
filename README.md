# Distributed Hash Table

## Overview
This is JaneZ's PPCA project,ACM Honor Class 2024.

In a P2P network, everyone is both a provider and a user of services—meaning everyone can potentially act as a server.

DHT enhances its stability by eliminating central servers and also improves the time efficiency.

## Chord Protocol
### Design
```
type ChordNode struct {
	Addr   string
	online bool
	ID     *big.Int
	QuitLock sync.RWMutex
	network.NetworkStation
	predecessor   string
	preLock       sync.RWMutex
	successorList [n + 1]string
	suLock        sync.RWMutex
	data     map[string]string
	dataLock sync.RWMutex
	dataBackup     map[string]string
	dataBackupLock sync.RWMutex
	fingerTable [m + 1]string
	fingerLock  sync.RWMutex
}
```
The function `maintain` operates continuously throughout a node's lifecycle.
```
func (node *ChordNode) maintain() {
	go func() {
		for {
			node.QuitLock.Lock()
			if !node.online {
				node.QuitLock.Unlock()
				break
			}
			node.Stabilize()
			node.QuitLock.Unlock()
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
```

### Problems Met
- **Dead Lock**: not an obvious one,happens mainly due to a mistake in `UpdateSuccessorList`
- **Interval judgment**: special judge lack
- **Wrong with data upd**: in `Stabilize`

## Kademlia Protocol
### Design
```
type KademliaNode struct {
	online bool
	network.NetworkStation
	BucketList   [m + 1]Bucket
	Addr         string
	ID           *big.Int
	data         Data
	refreshIndex int
}
```
The most important function `Lookup`,aiming to get the closest k nodes of a specific node:
```
func (node *KademliaNode) Lookup(key *big.Int) (kElem []string) {
	Sequence := SortList{}
	Sequence.Initialize(key, node.Addr)
	var firstKElem []string
	node.FindNode(key, &firstKElem)
	for i := range firstKElem {
		Sequence.Insert(firstKElem[i])
	}
	for {
		var callList []string
		callList = Sequence.GetFirstThree()
		closest := Sequence.GetFront()
		node.CallConcurrency(callList, &Sequence, key)
		if Sequence.IsEmpty() || Sequence.GetFront() == closest {
			callList = Sequence.GetAllUncall()
			node.CallConcurrency(callList, &Sequence, key)
			kElem = Sequence.GetFirstK()
			break
		}
	}
	return kElem
}
```
The k-bucket of kademlia is accomplished in LRU-K cache policy.

### Problems Met
- **Wrong Set with `k`** : firstly set k as 20,but almost killed my WSL(terrible experience).A simulated distributed system should ideally run on multiple machines. Setting the k-bucket size to 20 is only reasonable in such a scenario. If you run it on a single machine with a k-bucket size of 20, it will lead to excessive concurrency and cause WSL to crash. Therefore, a size of 8 or 10 is more appropriate.
- **Mistakes in DoubleList**: naive. Actually you can use `container/list`.

## Application
BitTorrent is a protocol for downloading and distributing files across the Internet. In contrast with the traditional client/server relationship, in which downloaders connect to a central server (for example: watching a movie on Netflix, or loading the web page you’re reading now), participants in the BitTorrent network, called peers, download pieces of files from each other—this is what makes it a peer-to-peer protocol.

Below are the main functions:
```
package main

func upload(inputPath, outputPath string, node *dhtNode) error {}

func putPieces(node *dhtNode, index int, data []byte, ch chan UploadInfo) {}

func download(inputPath, outputPath string, node *dhtNode) error {}

func getPieces(node *dhtNode, index int, hash [20]byte, ch chan DownloadInfo) {}
```

## Acknowledgement
1. Chuxi Wu (TA)
2. Alice
3. VitalRubbish
4. Tomorrow
5. WYT