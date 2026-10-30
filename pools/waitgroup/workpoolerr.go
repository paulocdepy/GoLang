package waitgroup

import (
	"fmt"
	"sync"
	"time"
)

type ResultadoWPE struct {
	Job int
	Err error
}

func processar(job int) error {
	time.Sleep(300 * time.Millisecond)

	if job == 3 {
		return fmt.Errorf("falha simulada no boleto %d", job)
	}

	return nil
}

func workerWPE(
	id int,
	jobs <-chan int,
	resultados chan<- ResultadoWPE,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for job := range jobs {
		fmt.Printf("Worker %d iniciou Job %d\n", id, job)

		err := processar(job)

		// Envia o resultado, inclusive quando há erro.
		resultados <- ResultadoWPE{
			Job: job,
			Err: err,
		}

		if err != nil {
			fmt.Printf("Worker %d: erro no Job %d\n", id, job)
			continue
		}

		fmt.Printf("Worker %d concluiu Job %d\n", id, job)
	}

	fmt.Printf("Worker %d encerrou.\n", id)
}

func CallWorkPoolErr() {
	const totalJobs = 6
	const totalWorkers = 3

	jobs := make(chan int)
	resultados := make(chan ResultadoWPE)

	var wg sync.WaitGroup
	wg.Add(totalWorkers)

	for id := 1; id <= totalWorkers; id++ {
		go workerWPE(id, jobs, resultados, &wg)
	}

	// Produtor: envia os Jobs e fecha a fila.
	go func() {
		defer close(jobs)

		for job := 1; job <= totalJobs; job++ {
			jobs <- job
		}
	}()

	// Fecha resultados somente após todos os Workers.
	go func() {
		wg.Wait()
		close(resultados)
	}()

	sucessos := 0
	falhas := 0

	for resultado := range resultados {
		if resultado.Err != nil {
			falhas++
			fmt.Printf(
				"MAIN: Job %d falhou: %v\n",
				resultado.Job,
				resultado.Err,
			)
			continue
		}

		sucessos++
		fmt.Printf("MAIN: Job %d processado com sucesso\n", resultado.Job)
	}

	fmt.Printf("\nResumo: %d sucessos, %d falhas.\n", sucessos, falhas)
}
