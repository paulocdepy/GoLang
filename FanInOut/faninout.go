package main

import (
	"fmt"
	"sync"
	"time"
)

type Job struct {
	ID int
}

type Resultado struct {
	WorkerID int
	JobID    int
}

func worker(
	id int,
	jobs <-chan Job,
	resultados chan<- Resultado,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for job := range jobs {

		fmt.Printf(
			"Worker %d iniciou Job %d\n",
			id,
			job.ID,
		)

		// Simula o processamento
		time.Sleep(100 * time.Millisecond)

		resultados <- Resultado{
			WorkerID: id,
			JobID:    job.ID,
		}

		fmt.Printf(
			"Worker %d terminou Job %d\n",
			id,
			job.ID,
		)
	}
}

func main() {

	const numeroWorkers = 3
	const numeroJobs = 10

	jobs := make(chan Job, 5)
	resultados := make(chan Resultado, 5)

	var wg sync.WaitGroup

	// FAN-OUT
	wg.Add(numeroWorkers)

	for i := 1; i <= numeroWorkers; i++ {
		go worker(i, jobs, resultados, &wg)
	}

	// Produz os Jobs
	for i := 1; i <= numeroJobs; i++ {
		jobs <- Job{ID: i}
	}

	close(jobs)

	// Goroutine responsável por coordenar
	// o fechamento do canal de resultados.
	go func() {
		wg.Wait()
		close(resultados)
	}()

	// FAN-IN
	for resultado := range resultados {

		fmt.Printf(
			"FAN-IN recebeu: Worker %d -> Job %d\n",
			resultado.WorkerID,
			resultado.JobID,
		)
	}

	fmt.Println("Todos os resultados foram recebidos.")
}
