package waitgroup

import (
	"fmt"
	"sync"
	"time"
)

func workerGracioso(
	id int,
	jobs <-chan int,
	resultados chan<- int,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for job := range jobs {
		fmt.Printf("Worker %d iniciou Job %d\n", id, job)

		// Simula o processamento do Job.
		time.Sleep(500 * time.Millisecond)

		// Entrega o resultado antes de buscar outro Job.
		resultados <- job
		fmt.Printf("Worker %d concluiu Job %d\n", id, job)
	}

	fmt.Printf("Worker %d encerrou.\n", id)
}

func CallEncerramentoGracioso() {
	const totalJobs = 6
	const totalWorkers = 3

	jobs := make(chan int)
	resultados := make(chan int)

	var wg sync.WaitGroup
	wg.Add(totalWorkers)

	for id := 1; id <= totalWorkers; id++ {
		go workerGracioso(id, jobs, resultados, &wg)
	}

	// Produtor: envia todos os Jobs e fecha a fila.
	go func() {
		defer close(jobs)

		for job := 1; job <= totalJobs; job++ {
			jobs <- job
			fmt.Printf("MAIN enviou Job %d\n", job)
		}
	}()

	// Encerramento dos resultados.
	go func() {
		wg.Wait()
		close(resultados)
	}()

	// Consumidor: permanece ativo durante o processamento.
	concluidos := 0

	for job := range resultados {
		concluidos++
		fmt.Printf(
			"MAIN recebeu resultado do Job %d (%d/%d)\n",
			job, concluidos, totalJobs,
		)
	}

	fmt.Printf(
		"MAIN terminou: %d de %d resultados recebidos.\n",
		concluidos, totalJobs,
	)
}
