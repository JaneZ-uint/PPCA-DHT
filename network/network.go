package network

import (
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type NetworkStation struct {
	listener  net.Listener
	listening bool
	server    *rpc.Server
	addr      string
}

func (node *NetworkStation) Init(Addr string) {
	node.addr = Addr
}

func (node *NetworkStation) RunRPCServer(chordNode interface{}, wg *sync.WaitGroup) {
	node.server = rpc.NewServer()
	node.server.Register(chordNode)
	var err error
	node.listener, err = net.Listen("tcp", node.addr)
	wg.Done()
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	for node.listening {
		conn, err := node.listener.Accept()
		if err != nil {
			logrus.Error("accept error: ", err)
			return
		}
		go node.server.ServeConn(conn)
	}
}

func (node *NetworkStation) StopRPCServer() {
	node.listening = false
	node.listener.Close()
}

func (node *NetworkStation) Run(origin interface{}, wg *sync.WaitGroup) {
	node.listening = true
	node.RunRPCServer(origin, wg)
}

// RemoteCall
// 是否建立 connection pool 待定
func (node *NetworkStation) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "ChordNode.Ping" {
		logrus.Infof("[%s] RemoteCall %s %s %v", node.addr, addr, method, args)
	}
	// Note: Here we use DialTimeout to set a timeout of 10 seconds.
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		logrus.Error("dialing: ", err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Error("RemoteCall error: ", err)
		return err
	}
	return nil
}
