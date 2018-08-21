package task

import (
	"sync"
)

//Worker must be implemented by types that want to
//Submit tasks to the worker pool
type Worker interface {
	// Task a worker executes a Task
	Task()
}

//Pool is a pool of goroutines that can execute tasks
//That are submitted by a worker
type Pool struct {
	work     chan Worker
	started  bool
	nworkers int
	wg       sync.WaitGroup
}

//New creates a new unstarted worker pool
//use Start to start the pool before use
func New(numWorkers int) *Pool {
	if numWorkers <= 0 {
		return nil
	}
	return &Pool{work: make(chan Worker), nworkers: numWorkers}
}

//Start starts a pool of workers
func (p *Pool) Start() {
	p.wg.Add(p.nworkers)
	for i := 0; i < p.nworkers; i++ {
		//worker threads wait for tasks on channel
		//and terminate when the channel is closed
		go func(id int) {
			//fmt.Printf("Worker [%d] starting...\n", id)
			for w := range p.work {
				w.Task()
			}
			//fmt.Printf("Worker [%d] terminated\n", id)
			p.wg.Done()
		}(i)
	}
	p.started = true
}

// Internal Method for Unit testing
func (p *Pool) isStarted() bool {
	return p.started
}

//Submit submits a task to be run by a worker in the pool
//this is a synchronous method and could block if all workers
//are busy. Panics if you submit a job to a stopped pool
func (p *Pool) Submit(w Worker) {
	p.work <- w
}

//Stop stops (gracefully) the pool, stopping a stopped pool causes a panic
func (p *Pool) Stop() {
	close(p.work)
	p.started = false
	p.wg.Wait()
}
