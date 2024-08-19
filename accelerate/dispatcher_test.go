package accelerate

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// 需要执行任务的结构体
type Task struct {
	Number int
}

// 实现Job这个interface的RunTask函数
func (t *Task) Do() {
	fmt.Println("This is task: ", t.Number)
	//设置个等待时间
	time.Sleep(1 * time.Second)
}

func TestNewWorkerPool(t *testing.T) {

	// 设置线程池的大小
	poolNum := 100 * 100 * 20
	jobQueueNum := 100
	dispatcher := NewDispatcher(poolNum, jobQueueNum)
	dispatcher.Start()

	// 模拟百万请求
	dataNum := 100 * 100 * 100

	go func() {
		for i := 0; i < dataNum; i++ {
			task := &Task{Number: i}
			dispatcher.Enqueue(task)
		}
	}()

	// 阻塞主线程
	for {
		fmt.Println("runtime.NumGoroutine() :", runtime.NumGoroutine())
		time.Sleep(2 * time.Second)
	}
}
