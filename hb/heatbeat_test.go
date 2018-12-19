package hb

import (
	"fmt"
	"testing"
	"time"
)

func TestHbEvent(t *testing.T) {
	event := NewHeartBeatEvent("m1", "u1", 1*time.Second)
	isActive := event.IsActive()
	if !isActive {
		t.Fail()
	}

	time.Sleep(1 * time.Second)
	isActive = event.IsActive()
	if isActive {
		t.Fail()
	}
}

func TestStr2Bytes(t *testing.T) {
	str1 := "abc"
	b1 := str2bytes(str1)
	if string(b1) != str1 {
		t.Fail()
	}

	str2 := "1234567890987654321abcdefghijklmnopqrstuvwxwz"
	b2 := str2bytes(str2)
	if string(b2) != str2 {
		t.Fail()
	}
}

func TestHashFunc(t *testing.T) {
	str := "1234567890987654321abcdefghijklmnopqrstuvwxwz"
	i := hashFunc(str)

	str2 := "1234567890987654321abcdefghijklmnopqrstuvwxwz"
	i2 := hashFunc(str2)

	t.Logf("i = %d\n", i)

	if i != i2 {
		t.Fail()
	}
}

func TestAttachHb(t *testing.T) {
	hbservice := NewHeartBeatService(1*time.Second, time.Second, 3)
	defer hbservice.Stop()
	evt, err := hbservice.Attach("u001", "m001")
	if err != nil {
		t.Fail()
	}
	if evt.status != STATUS_ACTIVE {
		t.Fail()
	}
	t.Logf("t1 last: %s, ret_code: %d \n", evt.GetLast(), evt.retCode)

	time.Sleep(500 * time.Millisecond)

	evt2, err2 := hbservice.Attach("u002", "m001")
	if err2 != nil {
		t.Fail()
	}
	if evt2.status != STATUS_WAITING {
		t.Fail()
	}

	t.Logf("t2 last: %s, ret_code: %d \n", evt2.GetLast(), evt2.retCode)

	time.Sleep(500 * time.Millisecond)
	evt3, err3 := hbservice.Attach("u003", "m001")
	if err3 == nil {
		t.Fail()
	}
	if evt3 == nil {
		t.Fail()
	}

	if evt3.retCode != RET_CODE_REJECT {
		t.Fail()
	}

	t.Logf("t3 last: %s, ret_code: %d \n", evt3.GetLast(), evt3.retCode)

	//这里会导致超时, 重新attach的话,相当于重新注册
	time.Sleep(4 * time.Second)
	evt4, err4 := hbservice.Attach("u002", "m001")
	if err4 != nil {
		t.Fail()
	}
	if evt4.status != STATUS_ACTIVE {
		t.Fail()
	}
	t.Logf("t2(1) last: %s, ret_code: %d \n", evt4.GetLast(), evt4.retCode)

	time.Sleep(4 * time.Second)
	//此时u0022跃升为actice
	evt44, err44 := hbservice.Attach("u0022", "m001")
	if err44 != nil {
		t.Fail()
	}
	if evt44.status != STATUS_ACTIVE {
		t.Fail()
	}
	t.Logf("t2(2) last: %s, ret_code: %d \n", evt44.GetLast(), evt44.retCode)

	evt, err = hbservice.Attach("u001", "m001")
	if err != nil {
		t.Fail()
	}
	if evt.status != STATUS_WAITING {
		t.Fail()
	}
	t.Logf("t1(1) last: %s, ret_code: %d \n", evt.GetLast(), evt.retCode)

	evt, err = hbservice.Attach("u001", "m001")
	if err != nil {
		t.Fail()
	}
	if evt.status != STATUS_WAITING {
		t.Fail()
	}
	t.Logf("t1(2) last: %s, ret_code: %d \n", evt.GetLast(), evt.retCode)

	evt, err = hbservice.Attach("uu001", "m001")
	if err == nil {
		t.Fail()
	}
	if evt.status != STATUS_INVALID {
		t.Fail()
	}
	t.Logf("t1(3) last: %s, ret_code: %d \n", evt.GetLast(), evt.retCode)

	evt5, err := hbservice.Attach("u005", "m005")
	if err != nil {
		t.Fail()
	}
	if evt5.status != STATUS_ACTIVE {
		t.Fail()
	}

	if evt5.retCode != RET_CODE_ACTIVE_ACCEPT {
		t.Fail()
	}

	t.Logf("t5 last: %s, ret_code: %d \n", evt5.GetLast(), evt5.retCode)

}

func TestHbEventChange(t *testing.T) {
	hbservice := NewHeartBeatService(1*time.Second, time.Second, 3)
	defer hbservice.Stop()

	evt, err := hbservice.Attach("u001", "m001")

	t.Logf("evt1 = %v\n", evt)

	if err != nil {
		t.Fail()
	}

	if evt.status != STATUS_ACTIVE {
		t.Fail()
	}

	time.Sleep(2 * time.Second)

	evt2, _ := hbservice.Attach("u002", "m001")

	t.Logf("evt2 = %v\n", evt2)

	if evt.status == evt2.status {
		t.Fail()
	}

	actives := hbservice.FindAllActiveEvents()
	for _, a := range actives {
		t.Logf("---active : %v\n", a.info)
	}
	waiting := hbservice.FindAllWaitingEvents()

	for _, a := range waiting {
		t.Logf("---waiting : %v\n", a.info)
	}

	time.Sleep(4 * time.Second)

	evt3, _ := hbservice.Attach("u002", "m001")

	t.Logf("evt3 = %v\n", evt3)

	if STATUS_ACTIVE != evt3.status {
		t.Fail()
	}

	evt4, _ := hbservice.Attach("u001", "m001")
	t.Logf("evt4 = %v\n", evt4)

	actives = hbservice.FindAllActiveEvents()
	for _, a := range actives {
		t.Logf("---active : %v\n", a.info)
	}
	waiting = hbservice.FindAllWaitingEvents()

	for _, a := range waiting {
		t.Logf("---waiting : %v\n", a.info)
	}
}

func TestActiveCheck(t *testing.T) {
	hbservice := NewHeartBeatService(1*time.Second, 1*time.Second, 3)
	defer hbservice.Stop()

	evt, err := hbservice.Attach("test-a-001", "test-a-mid")
	if err != nil {
		t.Fail()
	}
	if evt.status != STATUS_ACTIVE {
		t.Fail()
	}

	time.Sleep(4 * time.Second)
	hbservice.Attach("test-a-002", "test-a-mid")

	println("task start")

	time.Sleep(4 * time.Second)

	println("task end")
}

func TestFindByMid(t *testing.T) {
	hbservice := NewHeartBeatService(1*time.Second, 3*time.Second, 3)
	defer hbservice.Stop()

	testMid := "mid-a"
	_, ok := hbservice.FindByMid(testMid)

	if ok {
		t.Fail()
	}

	hbservice.Attach("test-uid", testMid)

	evt, ok := hbservice.FindByMid(testMid)

	if !ok {
		t.Fail()
	}

	if evt.IsTimeout() {
		t.Fail()
	}

	if evt.info.Mid != testMid {
		t.Fail()
	}

	if evt.info.Uid != "test-uid" {
		t.Fail()
	}

	if evt.status != STATUS_ACTIVE {
		t.Fail()
	}

	hbservice.Attach("test-uid-2", testMid)

	time.Sleep(10 * time.Second)

	_, ok = hbservice.FindByMid(testMid)

	if ok {
		t.Fail()
	}

	hbservice.Attach("test-uid-3", testMid)

}

func TestCallBack(t *testing.T) {
	hbservice := NewHeartBeatService(1*time.Second, 1*time.Second, 3)
	defer hbservice.Stop()

	hbservice.AddOnlineCallBacks(func(evt *HeartbeatEvent) {
		t.Logf(">>>>> online1 : %v\n", evt)
	}, func(evt *HeartbeatEvent) {
		t.Logf(">>>>> online2 : %v\n", evt)
	})

	hbservice.AddOfflineCallBacks(func(evt *HeartbeatEvent) {
		t.Logf("===== offline : %v\n", evt)
	})

	hbservice.Attach("uid", "mid")

	time.Sleep(4 * time.Second)

}

func BenchmarkStr2Bytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		str := "1234567890987654321abcdefghijklmnopqrstuvwxwz"
		str2bytes(str)
	}
}

func BenchmarkStr2Bytes2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		str := "1234567890987654321abcdefghijklmnopqrstuvwxwz"
		_ = []byte(str)
	}
}

func BenchmarkHbEvent(b *testing.B) {
	for i := 0; i < b.N; i++ {
		event := &HeartbeatEvent{}
		event.IsActive()
	}
}

func BenchmarkHashFunc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		str := "1234567890987654321abcdefghijklmnopqrstuvwxwz"
		_ = hashFunc(str) % 32
	}
}

func BenchmarkAttach(b *testing.B) {
	hbservice := NewHeartBeatService(1*time.Second, time.Second, 3)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		uid := fmt.Sprintf("uid-%d", i)
		mid := fmt.Sprintf("mid-%d", i)
		hbservice.Attach(uid, mid)
	}
	b.StopTimer()
	actives := hbservice.FindAllActiveEvents()
	b.Logf("active count : %d\n", len(actives))
	hbservice.Stop()
}
