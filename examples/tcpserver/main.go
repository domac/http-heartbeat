package main

import (
	"context"
	"github.com/domac/http-heartbeat/hb"
	"log"
	"net"
	"net/rpc"
	"time"
)

type ConfigTask struct {
}

func (c *ConfigTask) Sync(ctx context.Context) {

}
func (c *ConfigTask) Close() error {
	return nil
}
func (c *ConfigTask) Name() string {
	return "ConfigTime"
}
func (c *ConfigTask) Flush() error {
	return nil
}
func (c *ConfigTask) QueryById(id string) (interface{}, error) {
	return 99999, nil
}

func init() {

	hb.DefaultHeartBeatService = hb.NewHeartBeatService(1*time.Second, time.Second*5, 3)

	manager := hb.NewTaskSyncManager()
	testTask := hb.NewTestSyncTask()
	manager.AddTaskSyncs(testTask)
	manager.AddTaskSyncs(&ConfigTask{})

	hb.DefaultHeartBeatService.Schedule(manager)

	hb.DefaultHeartBeatService.AddOnlineCallBacks(func(evt *hb.HeartbeatEvent) {
		log.Printf("RPC Online : mid=%s, uid=%s, last=%s, next=%s\n", evt.GetInfo().Mid, evt.GetInfo().Uid, evt.GetLast(), evt.GetNext())
		m := hb.DefaultHeartBeatService.GetTaskManager().FindInfosById(evt.GetInfo().Mid)
		log.Printf(">>> test=> %v\n", m["TaskTime"])
		log.Printf(">>> config=> %v\n", m["ConfigTime"])
	})

	hb.DefaultHeartBeatService.AddOfflineCallBacks(func(evt *hb.HeartbeatEvent) {
		log.Printf("RPC Offline : mid=%s, uid=%s, last=%s, next=%s\n", evt.GetInfo().Mid, evt.GetInfo().Uid, evt.GetLast(), evt.GetNext())
	})
}

func main() {
	rpc.Register(new(hb.IConn))
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:10029")
	listener, _ := net.ListenTCP("tcp", tcpAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}
