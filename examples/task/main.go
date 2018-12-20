package main

import (
	"context"
	"github.com/domac/http-heartbeat/hb"
	"log"
	"net/http"
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
		log.Printf("Online : mid=%s, uid=%s, last=%s, next=%s\n", evt.GetInfo().Mid, evt.GetInfo().Uid, evt.GetLast(), evt.GetNext())
		m := hb.DefaultHeartBeatService.GetTaskManager().FindInfosById(evt.GetInfo().Mid)
		log.Printf(">>> test=> %v\n", m["TaskTime"])
		log.Printf(">>> config=> %v\n", m["ConfigTime"])
	})

	hb.DefaultHeartBeatService.AddOfflineCallBacks(func(evt *hb.HeartbeatEvent) {
		log.Printf("Offline : mid=%s, uid=%s, last=%s, next=%s\n", evt.GetInfo().Mid, evt.GetInfo().Uid, evt.GetLast(), evt.GetNext())
	})
}

func main() {
	http.HandleFunc("/hb", hb.HeartBeatCgi)
	http.HandleFunc("/hb/active", hb.HeartBeatStatusActivesCgi)
	http.HandleFunc("/hb/waiting", hb.HeartBeatStatusWaitingCgi)
	log.Println("start hb server")
	http.ListenAndServe(":10029", nil)
	hb.DefaultHeartBeatService.Stop()
}
