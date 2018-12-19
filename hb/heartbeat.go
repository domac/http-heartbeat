package hb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var (
	ErrHbTimeout         = errors.New("heartbeat event timeout")
	ErrInvalidId         = errors.New("invalid id request")
	ErrInvalidHb         = errors.New("heartbeats request fail, please use another mid")
	ErrHbServiceShutdown = errors.New("heartbeats service was shutdown")
	ErrTaskManagerNull   = errors.New("task manager is null")
)

//默认心跳服务
var DefaultHeartBeatService *HeartBeatService

const (
	SHARDS_NUM          = 32           //存储分片数
	EVENT_CHAN_SIZE     = 4096         //事件任务通道缓冲区大小
	DEFAULT_RETRY_COUNT = 3            //心跳重试次数
	HeatBeatsCmd        = uint32(5103) //接口请求参数指定命令字
)

const (
	RET_CODE_ACTIVE_ACCEPT  = 1  //心跳接受
	RET_CODE_ACTIVE         = 11 //心跳正常
	RET_CODE_WAITING_ACCEPT = 2  //心跳就绪接受
	RET_CODE_WAITING        = 22 //心跳就绪
	RET_CODE_REJECT         = -1 //心跳拒绝
)

const (
	STATUS_INIT    HB_STATUS = iota
	STATUS_ACTIVE            //心跳正常状态
	STATUS_WAITING           //心跳就绪状态
	STATUS_INVALID           //不合规状态
	STATUS_EXIT              //退出状态
)

type (
	RET_CODE int //返回码类型

	HbCallBackFunc func(evt *HeartbeatEvent) //任务钩子类型

	//心跳状态
	HB_STATUS int

	//心跳包信息
	HeartBeatInfo struct {
		Mid    string //业务编号
		Uid    string //唯一标识
		IpPort string
	}

	//心跳时间
	HeartbeatEvent struct {
		info       *HeartBeatInfo //心跳携带信息
		clientAddr string         //客户端地址
		rate       time.Duration  //心跳rate
		last       time.Time      //上一次心跳时间
		status     HB_STATUS
		retCode    RET_CODE

		counter uint32 //计数器
	}

	//心跳交换信息
	HBMessage struct {
		HbInterval uint32                 `json:"HbInterval"`
		ClientTime uint64                 `json:"ClientTime"`
		ServerTime uint64                 `json:"ServerTime"`
		Uid        uint64                 `json:"Uid"`
		Info       map[string]interface{} `json:"Info"`
	}

	Result struct {
		RetCode    int    `json:"ret_code"`
		RetMessage string `json:"ret_msg"`
		Data       []byte `json:"data"`
	}
)

//创建心跳事件
func NewHeartBeatEvent(mid, uid string, rate time.Duration) *HeartbeatEvent {
	event := &HeartbeatEvent{
		last: time.Now(),
		rate: rate,
		info: &HeartBeatInfo{
			Mid: mid,
			Uid: uid,
		},
	}
	return event
}

func (event *HeartbeatEvent) addCounter() uint32 {
	atomic.AddUint32(&event.counter, 1)
	return event.counter
}

func (event *HeartbeatEvent) cleanCounter() {
	atomic.StoreUint32(&event.counter, 0)
}

func (event *HeartbeatEvent) getCounter() uint32 {
	return atomic.LoadUint32(&event.counter)
}

//是否活跃事件
func (event *HeartbeatEvent) IsActive() bool {
	if event.last.Add(event.rate).After(time.Now()) {
		return true
	}
	return false
}

//获取基础信息
func (event *HeartbeatEvent) GetInfo() *HeartBeatInfo {
	return event.info
}

//是否超时事件
func (event *HeartbeatEvent) IsTimeout() bool {
	return !event.IsActive()
}

//获取最近一次心跳时间
func (event *HeartbeatEvent) GetLast() string {
	return event.last.Format("2006-01-02 15:04:05")
}

//获取下一次要正常心跳的时间
func (event *HeartbeatEvent) GetNext() string {
	return event.last.Add(event.rate).Format("2006-01-02 15:04:05")
}

//心跳服务
type HeartBeatService struct {
	activeLock    [SHARDS_NUM]sync.RWMutex
	ActiveEvents  [SHARDS_NUM]map[string]*HeartbeatEvent
	waitingLock   [SHARDS_NUM]sync.RWMutex
	WaitingEvents [SHARDS_NUM]map[string]*HeartbeatEvent

	checkPeriod time.Duration
	rate        time.Duration

	stopChan chan struct{}
	running  bool //服务运行状态
	retry    int

	eventNotifyChan chan *HeartbeatEvent //任务队列

	onlineCallBack  []HbCallBackFunc //上线回调
	offlineCallBack []HbCallBackFunc //离线回调

	maxTryCount uint32

	taskManager *SyncTaskManager
}

//根据检测间隔和心跳频率构造心跳服务
func NewHeartBeatService(checkPeriod time.Duration, rate time.Duration, retry int) *HeartBeatService {

	if retry < 0 {
		retry = DEFAULT_RETRY_COUNT
	}

	//心跳行为的最大尝试次数 = 最大超时间距/QPS + 1
	maxTryCount := ((rate * time.Duration(retry)) / time.Second) + 1

	hbs := &HeartBeatService{
		checkPeriod:     checkPeriod,
		rate:            rate,
		stopChan:        make(chan struct{}, 1),
		eventNotifyChan: make(chan *HeartbeatEvent, EVENT_CHAN_SIZE),
		maxTryCount:     uint32(maxTryCount),
		retry:           retry,
	}
	for i := 0; i < SHARDS_NUM; i++ {
		hbs.ActiveEvents[i] = make(map[string]*HeartbeatEvent)
		hbs.WaitingEvents[i] = make(map[string]*HeartbeatEvent)
	}
	go hbs.handleEventNotify()
	hbs.keepalive()
	hbs.running = true
	return hbs
}

//定时心跳检测
func (hb *HeartBeatService) keepalive() {
	activeTicker := time.NewTicker(hb.checkPeriod)
	waitingTicker := time.NewTicker(hb.checkPeriod)

	//定时active检测
	go func() {
		for {
			select {
			case <-activeTicker.C:
				hb.activeCheck()
			case <-hb.stopChan:
				activeTicker.Stop()
				return
			}
		}
	}()

	//定时waiting检测
	go func() {
		for {
			select {
			case <-waitingTicker.C:
				hb.waitingCheck()
			case <-hb.stopChan:
				waitingTicker.Stop()
				return
			}
		}
	}()
}

//处理事件通知
//目前处理两类事件:上线;离线
func (hb *HeartBeatService) handleEventNotify() {
	for {
		select {
		case evt := <-hb.eventNotifyChan: //事件通知
			if evt.status == STATUS_ACTIVE {
				for _, fn := range hb.onlineCallBack {
					fn(evt)
				}
			} else if evt.status == STATUS_EXIT {
				for _, fn := range hb.offlineCallBack {
					fn(evt)
				}
			}
		case <-hb.stopChan:
			return
		}
	}
}

func (hb *HeartBeatService) activeCheck() {
	for i := 0; i < SHARDS_NUM; i++ {
		//异步处理
		go func(i int) {
			activeEvents := hb.ActiveEvents[i]
			hb.activeLock[i].RLock()
			tasks := make(map[string]*HeartbeatEvent)
			for mid, evt := range activeEvents {
				if evt.IsTimeout() {
					evt.status = STATUS_EXIT
					tasks[mid] = evt
				}
			}
			hb.activeLock[i].RUnlock()
			if len(tasks) > 0 {
				for mid, evt := range tasks {
					hb.activeLock[i].Lock()
					delete(hb.ActiveEvents[i], mid)
					hb.activeLock[i].Unlock()
					hb.eventNotifyChan <- evt
				}
			}
		}(i)
	}
}

func (hb *HeartBeatService) waitingCheck() {
	for i := 0; i < SHARDS_NUM; i++ {
		go func(i int) {
			waitingEvents := hb.WaitingEvents[i]
			hb.waitingLock[i].RLock()

			tasks := make(map[string]*HeartbeatEvent)
			for mid, evt := range waitingEvents {
				if evt.IsActive() {
					hb.activeLock[i].RLock()
					_, hasActive := hb.ActiveEvents[i][mid]
					hb.activeLock[i].RUnlock()
					//活跃表中已经没有同mid元素
					if !hasActive {
						evt.status = STATUS_ACTIVE
						tasks[mid] = evt
					} else {
						//如果发现存在活跃的同一mid心跳
						continue
					}

				} else {
					evt.status = STATUS_EXIT
					tasks[mid] = evt
				}
			}
			hb.waitingLock[i].RUnlock()
			if len(tasks) > 0 {
				for mid, evt := range tasks {
					if evt.status == STATUS_ACTIVE {
						hb.activeLock[i].Lock()
						evt.cleanCounter()
						evt.last = time.Now()
						hb.ActiveEvents[i][mid] = evt
						hb.activeLock[i].Unlock()

						//清理waiting信息
						hb.waitingLock[i].Lock()
						delete(hb.WaitingEvents[i], mid)
						hb.waitingLock[i].Unlock()

						hb.eventNotifyChan <- evt

					} else if evt.status == STATUS_EXIT {

						//清理waiting信息
						hb.waitingLock[i].Lock()
						log.Printf("drop wait event : uid=%s, mid=%s\n", evt.info.Uid, evt.info.Mid)
						delete(hb.WaitingEvents[i], mid)
						hb.waitingLock[i].Unlock()
						//hb.eventNotifyChan <- evt
					}
				}
			}

		}(i)
	}
}

//心跳固定处理
func (hb *HeartBeatService) Attach(uid, mid string) (*HeartbeatEvent, error) {

	if hb.running == false {
		return nil, ErrHbServiceShutdown
	}

	if uid == "" || mid == "" {
		return nil, ErrInvalidId
	}

	idx := hashFunc(mid) % SHARDS_NUM
	rate := hb.rate * time.Duration(hb.retry)

	//判断是否在active中存在
	hb.activeLock[idx].RLock()
	evt, ok := hb.ActiveEvents[idx][mid]
	hb.activeLock[idx].RUnlock()
	if !ok {
		//active表中不存在,直接创建
		evt = NewHeartBeatEvent(mid, uid, rate)
		evt.status = STATUS_ACTIVE
		evt.retCode = RET_CODE_ACTIVE_ACCEPT
		hb.activeLock[idx].Lock()
		hb.ActiveEvents[idx][mid] = evt
		hb.activeLock[idx].Unlock()
		hb.eventNotifyChan <- evt
		return evt, nil
	} else {
		//同一个终端
		if uid == evt.info.Uid {
			//更新最后更新时间
			hb.activeLock[idx].Lock()
			hb.ActiveEvents[idx][mid].last = time.Now()
			hb.ActiveEvents[idx][mid].retCode = RET_CODE_ACTIVE
			hb.activeLock[idx].Unlock()
			return evt, nil
		}
	}

	//active表中存在, 判断是否在waiting中存在
	hb.waitingLock[idx].RLock()
	evt, ok = hb.WaitingEvents[idx][mid]
	hb.waitingLock[idx].RUnlock()
	if !ok {
		evt = NewHeartBeatEvent(mid, uid, rate)
		evt.status = STATUS_WAITING
		evt.retCode = RET_CODE_WAITING_ACCEPT
		hb.waitingLock[idx].Lock()
		hb.WaitingEvents[idx][mid] = evt
		hb.waitingLock[idx].Unlock()
		return evt, nil
	} else {

		//同一个终端(已经有相关请求在等待)
		//允许不超过最大等待尝试次数的情况下处理心跳
		if uid == evt.info.Uid && evt.getCounter() < hb.maxTryCount {
			//更新最后更新时间
			hb.waitingLock[idx].Lock()
			hb.WaitingEvents[idx][mid].addCounter()
			hb.WaitingEvents[idx][mid].last = time.Now()
			hb.WaitingEvents[idx][mid].retCode = RET_CODE_WAITING
			hb.waitingLock[idx].Unlock()
			return evt, nil
		}
	}

	evt = NewHeartBeatEvent(mid, uid, 0)
	evt.retCode = RET_CODE_REJECT
	evt.status = STATUS_INVALID
	return evt, ErrInvalidHb
}

//添加上线回调pipeline
func (hb *HeartBeatService) AddOnlineCallBacks(callback ...HbCallBackFunc) {
	hb.onlineCallBack = append(hb.onlineCallBack, callback...)
}

//添加离线回调pipeline
func (hb *HeartBeatService) AddOfflineCallBacks(callback ...HbCallBackFunc) {
	hb.offlineCallBack = append(hb.offlineCallBack, callback...)
}

func (hb *HeartBeatService) FindAllActiveEvents() []*HeartbeatEvent {
	result := make([]*HeartbeatEvent, 0)
	for i := 0; i < SHARDS_NUM; i++ {
		hb.activeLock[i].RLock()
		for _, v := range hb.ActiveEvents[i] {
			result = append(result, v)
		}
		hb.activeLock[i].RUnlock()
	}
	return result
}

func (hb *HeartBeatService) FindAllWaitingEvents() []*HeartbeatEvent {
	result := make([]*HeartbeatEvent, 0)
	for i := 0; i < SHARDS_NUM; i++ {
		hb.waitingLock[i].RLock()
		for _, v := range hb.WaitingEvents[i] {
			result = append(result, v)
		}
		hb.waitingLock[i].RUnlock()
	}
	return result
}

func (hb *HeartBeatService) FindByMid(mid string) (*HeartbeatEvent, bool) {
	idx := hashFunc(mid) % SHARDS_NUM
	hb.activeLock[idx].RLock()
	evt, ok := hb.ActiveEvents[idx][mid]
	hb.activeLock[idx].RUnlock()
	if ok {
		return evt, true
	}
	hb.waitingLock[idx].RLock()
	evt, ok = hb.WaitingEvents[idx][mid]
	hb.waitingLock[idx].RUnlock()
	if ok {
		return evt, true
	}
	return nil, false
}

func (hb *HeartBeatService) Flush() {
	for i := 0; i < SHARDS_NUM; i++ {
		go func(i int) {
			hb.activeLock[i].Lock()
			hb.ActiveEvents[i] = make(map[string]*HeartbeatEvent)
			hb.activeLock[i].Unlock()

			hb.waitingLock[i].Lock()
			hb.WaitingEvents[i] = make(map[string]*HeartbeatEvent)
			hb.waitingLock[i].Unlock()

		}(i)
	}
}

//drain the chan to clean up online events
func (hb *HeartBeatService) drainNotify() {
	close(hb.eventNotifyChan)
	for range hb.eventNotifyChan {
	}
}

func (hb *HeartBeatService) Schedule(taskManager *SyncTaskManager) error {
	if taskManager == nil {
		return ErrTaskManagerNull
	}
	hb.taskManager = taskManager
	go hb.taskManager.Start()
	return nil
}

func (hb *HeartBeatService) GetTaskManager() *SyncTaskManager {
	return hb.taskManager
}

//关闭心跳服务
func (hb *HeartBeatService) Stop() {

	close(hb.stopChan)
	hb.running = false

	if hb.taskManager != nil {
		hb.taskManager.Stop()
	}
	time.Sleep(1 * time.Second)
}

func str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

//自定义哈希函数
func hashFunc(data string) uint64 {
	var h uint64 = 14695981039346656037
	for _, c := range data {
		h = (h ^ uint64(c)) * 1099511628211
	}
	return h
}

func string2Uint32(data string) uint32 {
	value, _ := strconv.ParseUint(data, 10, 0)
	return uint32(value)
}

func CreateResult(ret_code int, ret_msg string, data []byte) Result {
	return Result{
		RetCode:    ret_code,
		RetMessage: ret_msg,
		Data:       data,
	}
}

func (result Result) toJsonBytes() []byte {
	b, err := json.Marshal(result)
	if err != nil {
		return nil
	}
	return b
}

func (msg HBMessage) toJsonBytes() []byte {
	b, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return b
}

//全局心跳活跃状态api
func HeartBeatStatusActivesCgi(rspWriter http.ResponseWriter, req *http.Request) {
	if DefaultHeartBeatService != nil {
		actives := DefaultHeartBeatService.FindAllActiveEvents()
		idx := 0
		for _, active := range actives {
			idx++
			fmt.Fprintf(rspWriter, "%d) mid=%s, uid=%s, last=%s\n", idx, active.info.Mid, active.info.Uid, active.GetLast())
		}
		return
	} else {
		result := CreateResult(-2, "heartbeat server not working", nil).toJsonBytes()
		rspWriter.Write(result)
		return
	}
}

//全局心跳就绪状态api
func HeartBeatStatusWaitingCgi(rspWriter http.ResponseWriter, req *http.Request) {
	if DefaultHeartBeatService != nil {
		waitings := DefaultHeartBeatService.FindAllWaitingEvents()
		idx := 0
		for _, waiting := range waitings {
			idx++
			fmt.Fprintf(rspWriter, "%d) mid=%s, uid=%s, last=%s\n", idx, waiting.info.Mid, waiting.info.Uid, waiting.GetLast())
		}
		return
	} else {
		result := CreateResult(-2, "heartbeat server not working", nil).toJsonBytes()
		rspWriter.Write(result)
		return
	}
}

//心跳api
func HeartBeatCgi(rspWriter http.ResponseWriter, req *http.Request) {

	rspWriter.Header().Set("Proto-Ver", req.Header.Get("Proto-Ver"))
	rspWriter.Header().Set("Content-Seq", req.Header.Get("Content-Seq"))
	rspWriter.Header().Set("Content-Mid", req.Header.Get("Content-Mid"))
	rspWriter.Header().Set("Proto-Cmd", req.Header.Get("Proto-Cmd"))
	rspWriter.Header().Set("Content-Encrypt", req.Header.Get("Content-Encrypt"))
	rspWriter.Header().Set("Content-Type", req.Header.Get("Content-Type"))

	var hbMessage HBMessage

	//读取body
	bodybytes, err := ioutil.ReadAll(req.Body)
	if err == nil {
		json.Unmarshal(bodybytes, &hbMessage)
	} else {
		hbMessage = HBMessage{}
	}

	hbMessage.Info = make(map[string]interface{})
	hbMessage.ServerTime = uint64(time.Now().Unix())

	urlValues, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		rspWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	//获取命令字
	cmd := urlValues.Get("cmd")
	icmd := string2Uint32(cmd)
	if cmd == "" || icmd != HeatBeatsCmd {
		result := CreateResult(0, "cmd invalid", nil).toJsonBytes()
		rspWriter.Write(result)
		return
	}

	//获取Uid
	suid := strconv.Itoa(int(hbMessage.Uid))
	if suid == "0" {
		suid = urlValues.Get("uid")
		if suid == "" {
			hbMessage.Info["ret_code"] = -1
			hbMessage.Info["ret_msg"] = "uid must not empty"
			rspWriter.Header().Set("Client-Status", "6")
			//rspWriter.WriteHeader(http.StatusForbidden)
			rspWriter.Write(hbMessage.toJsonBytes())
			return
		}
	}

	//获取Mid
	mid := urlValues.Get("mid")
	if mid == "" {
		hbMessage.Info["ret_code"] = -1
		hbMessage.Info["ret_msg"] = "mid must not empty"
		rspWriter.Write(hbMessage.toJsonBytes())
		return
	}

	//是否初始化默认的心跳服务
	if DefaultHeartBeatService != nil {

		hbMessage.HbInterval = uint32(DefaultHeartBeatService.rate / time.Second)

		hb, err := DefaultHeartBeatService.Attach(suid, mid)
		if err != nil {
			//需要响应重新更换mid的报文回去
			hbMessage.Info["ret_code"] = -1
			hbMessage.Info["ret_msg"] = "register"
			rspWriter.Header().Set("Client-Status", "6")
			//rspWriter.WriteHeader(http.StatusForbidden)
			rspWriter.Write(hbMessage.toJsonBytes())
			return
		}

		hbMessage.Info["ret_code"] = hb.retCode
		hbMessage.Info["last"] = hb.GetLast()
		hbMessage.Info["ret_msg"] = "success"
		rspWriter.Write(hbMessage.toJsonBytes())
		return

	} else {
		hbMessage.Info["ret_code"] = -1
		hbMessage.Info["ret_msg"] = "heartbeat server was shutdown"
		//rspWriter.WriteHeader(http.StatusForbidden)
		rspWriter.Write(hbMessage.toJsonBytes())
		return
	}
}
