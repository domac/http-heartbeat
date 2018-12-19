package hb

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"
)

//任务同步接口
type ISyncTask interface {
	Sync(ctx context.Context)
	Close() error
	Name() string
	Flush() error
	QueryById(id string) ([]byte, error)
}

//任务同步管理器
type SyncTaskManager struct {
	tasks    []ISyncTask
	stopchan chan struct{}
}

func NewTaskSyncManager() *SyncTaskManager {
	return &SyncTaskManager{
		tasks:    make([]ISyncTask, 0),
		stopchan: make(chan struct{}, 1),
	}
}

//开启任务调度
func (manager *SyncTaskManager) Start() {
	ctx, cancelFunc := context.WithCancel(context.Background())
	for _, t := range manager.tasks {
		go t.Sync(ctx)
	}
	//停止通道
	<-manager.stopchan
	cancelFunc()
}

//终止任务调度
func (manager *SyncTaskManager) Stop() {
	close(manager.stopchan)
}

//添加同步任务
func (manager *SyncTaskManager) AddTaskSyncs(tasks ...ISyncTask) {
	manager.tasks = append(manager.tasks, tasks...)
}

//获取信息
func (manager *SyncTaskManager) FindInfosById(id string) map[string][]byte {
	result := make(map[string][]byte, len(manager.tasks))
	for _, t := range manager.tasks {
		b, err := t.QueryById(id)
		if err != nil {
			b = nil
		}
		result[t.Name()] = b
	}
	return result

}

type TestSyncTask struct {
	mu      sync.RWMutex
	testMap map[string]uint64
	counter uint64
}

func NewTestSyncTask() *TestSyncTask {
	test := &TestSyncTask{
		testMap: make(map[string]uint64),
	}
	return test
}

func (test *TestSyncTask) Sync(ctx context.Context) {
	timer := time.NewTimer(1 * time.Second)
	for {
		select {
		case <-timer.C:
			test.mu.Lock()
			cnt, ok := test.testMap["1024"]
			if !ok {
				cnt = 0
			}
			test.testMap["1024"] = cnt + 1
			timer.Reset(1 * time.Second)
			test.mu.Unlock()
		case <-ctx.Done():
			test.Close()
			return
		}
	}
}

func (test *TestSyncTask) Close() error {
	log.Println(">>>>>> task sync test close")
	return nil
}

func (test *TestSyncTask) Name() string {
	return "TestTask"
}

func (test *TestSyncTask) Flush() error {
	return nil
}

func (test *TestSyncTask) QueryById(id string) ([]byte, error) {
	data, ok := test.testMap[id]
	if !ok {
		return []byte("no counter"), nil
	}
	s := strconv.Itoa(int(data))
	return []byte(s), nil
}
