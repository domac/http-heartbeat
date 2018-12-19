# http-heartbeat
基于http的心跳管理服务

### 服务层使用参考

```go
func init() {
	hb.DefaultHeartBeatService = hb.NewHeartBeatService(1*time.Second, time.Second*5)

	hb.DefaultHeartBeatService.AddOnlineCallBacks(func(evt *hb.HeartbeatEvent) {
		//code 
	})

	hb.DefaultHeartBeatService.AddOfflineCallBacks(func(evt *hb.HeartbeatEvent) {
		//code
	})
}

func main() {
	http.HandleFunc("/hb", hb.HeartBeatCgi)
	http.HandleFunc("/hb/active", hb.HeartBeatStatusActivesCgi)
	http.HandleFunc("/hb/waiting", hb.HeartBeatStatusWaitingCgi)
	log.Println("start hb server")
	http.ListenAndServe(":10029", nil)
}
```

### 客户端上报参考

```
curl -d '{"Uid":20001,"HbInterval":5,"ClientTime":123}'  'http://localhost:10029/hb?cmd=5103&mid=1001'
```