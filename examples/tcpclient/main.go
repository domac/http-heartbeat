package main

import (
	"fmt"
	"github.com/domac/http-heartbeat/hb"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const (
	client_num    = 100
	request_count = 200000
)

func main() {
	rpcclients := []*rpc.Client{}
	for i := 0; i < client_num; i++ {
		conn, _ := net.Dial("tcp", "127.0.0.1:10029")
		client := rpc.NewClient(conn)
		rpcclients = append(rpcclients, client)
	}

	var wg sync.WaitGroup

	for i := 0; i < client_num; i++ {
		wg.Add(1)
		go func(i int) {
			hc := rpcclients[i]

			for j := 0; j < request_count; j++ {

				var req hb.MsgConnReqPkt
				var rsp hb.MsgConnRspPkt

				req.SetInfo("1024", "7777")
				req.SetInfo(fmt.Sprintf("%d", i), fmt.Sprintf("%d", i+1))
				hc.Call("IConn.OnData", &req, &rsp)

				log.Printf("rsp: %s\n", rsp.Data)
				time.Sleep(1 * time.Second)
			}
			wg.Done()

		}(i)
	}
	wg.Wait()

}
