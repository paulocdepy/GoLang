package waitgroup

import (
	"fmt"
	"sync"
	"time"
)

func worker(
	id int,
	jobs <-chan int,
	resultados chan<- int,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for job := range jobs {
		fmt.Printf("Worker %d iniciou Job %d\n", id, job)

		time.Sleep(200 * time.Millisecond)

		resultados <- job
		fmt.Printf("Worker %d concluiu Job %d\n", id, job)
	}
}

type Resultado struct {
	JobID int
	Erro  error
}

func CallResultWaitGr() {
	const totalJobs = 10
	const totalWorkers = 3

	jobs := make(chan int)
	resultados := make(chan int)

	var wg sync.WaitGroup
	wg.Add(totalWorkers)

	// Inicia os Workers.
	for i := 1; i <= totalWorkers; i++ {
		go worker(i, jobs, resultados, &wg)
	}

	// Produz os Jobs e fecha a fila.
	go func() {
		for job := 1; job <= totalJobs; job++ {
			jobs <- job
		}
		close(jobs)
	}()

	// Fecha resultados somente após todos os Workers terminarem.
	go func() {
		wg.Wait()
		close(resultados)
	}()

	// Consome todos os resultados.
	concluidos := 0

	for job := range resultados {
		concluidos++
		fmt.Printf(
			"MAIN recebeu resultado do Job %d (%d/%d)\n",
			job, concluidos, totalJobs,
		)
	}

	fmt.Printf(
		"Fim: %d de %d Jobs concluídos.\n",
		concluidos, totalJobs,
	)
}
