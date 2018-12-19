package main

import (
	"github.com/domac/http-heartbeat/hb"
	"log"
	"net/http"
	"time"
)

func init() {

	hb.DefaultHeartBeatService = hb.NewHeartBeatService(1*time.Second, time.Second*5, 3)

	manager := hb.NewTaskSyncManager()
	testTask := hb.NewTestSyncTask()
	manager.AddTaskSyncs(testTask)

	hb.DefaultHeartBeatService.Schedule(manager)

	hb.DefaultHeartBeatService.AddOnlineCallBacks(func(evt *hb.HeartbeatEvent) {
		log.Printf("Online : mid=%s, uid=%s, last=%s, next=%s\n", evt.GetInfo().Mid, evt.GetInfo().Uid, evt.GetLast(), evt.GetNext())
		m := hb.DefaultHeartBeatService.GetTaskManager().FindInfosById(evt.GetInfo().Mid)
		log.Printf(">>> test=> %v\n", m["TaskTime"])
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
