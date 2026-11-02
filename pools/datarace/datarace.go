package datarace

import (
	"fmt"
	"sync"
)

func registrarJob(
	mu *sync.Mutex,
	total *int,
	workerID int,
	job int,
) {
	mu.Lock()
	defer mu.Unlock()

	(*total)++

	fmt.Printf(
		"Worker %d registrou Job %d. Total: %d\n",
		workerID,
		job,
		*total,
	)
}

func worker(
	id int,
	jobs <-chan int,
	mu *sync.Mutex,
	total *int,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for job := range jobs {
		registrarJob(mu, total, id, job)
	}

	fmt.Printf("Worker %d terminou.\n", id)
}

func MainDataRace() {
	const totalJobs = 10
	const totalWorkers = 3

	jobs := make(chan int)

	var mu sync.Mutex
	var wg sync.WaitGroup

	totalProcessados := 0

	wg.Add(totalWorkers)

	for id := 1; id <= totalWorkers; id++ {
		go worker(
			id,
			jobs,
			&mu,
			&totalProcessados,
			&wg,
		)
	}

	for job := 1; job <= totalJobs; job++ {
		jobs <- job
	}

	close(jobs)

	wg.Wait()

	fmt.Println("---------------------------")
	fmt.Println("Total esperado:", totalJobs)
	fmt.Println("Total obtido:  ", totalProcessados)
}
