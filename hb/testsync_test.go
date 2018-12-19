package hb

import (
	"testing"
	"time"
)

func TestSyncTaskGet(t *testing.T) {
	manager := NewTaskSyncManager()
	testTask := NewTestSyncTask()
	manager.AddTaskSyncs(testTask)

	go manager.Start()

	m := manager.FindInfosById("1024")

	data, ok := m[testTask.Name()]
	if !ok {
		t.Fail()
	}

	t.Logf("1. %d\n", data)

	time.Sleep(time.Second * 5)

	m = manager.FindInfosById("1024")

	data, ok = m[testTask.Name()]
	if !ok {
		t.Fail()
	}

	t.Logf("2. %d\n", data)

	manager.Stop()

}
