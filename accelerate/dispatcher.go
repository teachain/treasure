package accelerate

// Dispatcher 线程池
type Dispatcher struct {
	// 线程池大小
	workerSize int
	// 不带缓冲的任务队列，任务到达后，从workerQueue随机选取一个Worker来执行Job
	jobQueue    chan Job
	workerQueue chan *Worker
}

func NewDispatcher(workerSize, jobQueueLen int) *Dispatcher {
	return &Dispatcher{
		workerSize:  workerSize,
		jobQueue:    make(chan Job, jobQueueLen),
		workerQueue: make(chan *Worker, workerSize),
	}
}

func (d *Dispatcher) Start() {
	// 将所有worker启动
	for i := 0; i < d.workerSize; i++ {
		worker := NewWorker()
		worker.Start(d)
	}
	// 监听JobQueue，如果接收到请求，随机取一个Worker，然后将Job发送给该Worker的JobQueue
	// 需要启动一个新的协程，来保证不阻塞
	go func() {
		for {
			select {
			case job := <-d.jobQueue:
				worker := <-d.workerQueue
				worker.jobQueue <- job
			}
		}
	}()
}

func (d *Dispatcher) Enqueue(job Job) {
	d.jobQueue <- job
}
