package accelerate

// Worker Worker结构体
// WorkerPool随机选取一个Worker，将Job发送给Worker去执行
type Worker struct {
	// 不需要带缓冲的任务队列
	jobQueue chan Job
	//退出标志
	stop chan struct{}
}

// NewWorker 创建一个新的Worker对象
func NewWorker() *Worker {
	return &Worker{
		jobQueue: make(chan Job),
		stop:     make(chan struct{}),
	}
}

// Start 启动一个Worker，来监听Job事件
// 执行完任务，需要将自己重新发送到WorkerPool
func (w *Worker) Start(workerPool *Dispatcher) {
	// 需要启动一个新的协程，从而不会阻塞
	go func() {
		for {
			// 将worker注册到线程池
			workerPool.workerQueue <- w
			select {
			case job := <-w.jobQueue:
				job.Do()
			// 终止当前worker
			case <-w.stop:
				return
			}
		}
	}()
}
