package main

import (
	"fmt"
	"sync"
	"time"
)

type Job struct {
	ID int
}

func worker(id int, jobs <-chan Job, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {

		fmt.Printf(
			"Worker %d iniciou o Job %d\n",
			id,
			job.ID,
		)

		time.Sleep(10 * time.Millisecond)

		fmt.Printf(
			"Worker %d terminou o Job %d\n",
			id,
			job.ID,
		)
	}
}

func main() {

	inicio := time.Now()

	jobs := make(chan Job, 100)

	var wg sync.WaitGroup

	// Temos 3 Workers
	wg.Add(20)

	for i := 1; i <= 20; i++ {
		go worker(i, jobs, &wg)
	}

	// Produzimos 9 Jobs
	for i := 1; i <= 1000; i++ {
		fmt.Printf("MAIN tentando enviar Job %d\n", i)
		jobs <- Job{ID: i}
		fmt.Printf("Main enviou Job %d\n", i)
	}

	// Não haverá mais Jobs
	close(jobs)

	// Espera os 3 Workers terminarem
	wg.Wait()

	fmt.Println("Todos os Workers terminaram.")

	fmt.Printf("Tempo Teste 5 -> 20 Worker, Buffer 100 e 1000 Jobs Worker: %v\n", time.Since(inicio))

}
