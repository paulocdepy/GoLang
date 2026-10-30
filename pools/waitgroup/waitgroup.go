package waitgroup

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func TimeWorkerPool() {
	jobs := make(chan int, 3)
	resultados := make(chan string)

	var wg sync.WaitGroup

	// Vamos criar 3 Workers.
	wg.Add(3)

	for i := 1; i <= 3; i++ {
		go workerPool(i, jobs, resultados, &wg)
	}

	// Envia os Jobs.
	go func() {
		for job := 1; job <= 10; job++ {
			jobs <- job
			fmt.Println("MAIN enviou Job", job)
		}

		close(jobs)
		fmt.Println("Canal jobs fechado.")
	}()

	// Quando TODOS os Workers terminarem,
	// fecha o canal de resultados.
	go func() {
		wg.Wait()
		close(resultados)
		fmt.Println("Canal resultados fechado.")
	}()

	// Recebe os resultados até resultados ser fechado.
	for resultado := range resultados {
		fmt.Println("MAIN recebeu:", resultado)
	}

	fmt.Println("MAIN: Worker Pool terminou.")
}

func workerPool(id int, jobs <-chan int, resultados chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		fmt.Printf("Worker %d recebeu Job %d\n", id, job)

		time.Sleep(1 * time.Second)

		resultados <- fmt.Sprintf(
			"Worker %d concluiu Job %d",
			id,
			job,
		)
	}

	fmt.Printf("Worker %d terminou.\n", id)
}

func WorkerPoolComContext() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan int)
	resultados := make(chan string)

	var wg sync.WaitGroup

	// 3 Workers
	wg.Add(3)

	for i := 1; i <= 3; i++ {
		go WorkerPoolContext(
			i,
			ctx,
			jobs,
			resultados,
			&wg,
		)
	}

	// Produtor de Jobs.
	go func() {
		for job := 1; job <= 10; job++ {
			select {

			case <-ctx.Done():
				fmt.Println("PRODUTOR: cancelado.")
				close(jobs)
				return

			case jobs <- job:
				fmt.Println("MAIN enviou Job", job)
			}
		}

		close(jobs)
		fmt.Println("Canal jobs fechado.")
	}()

	// Aguarda todos os Workers e fecha resultados.
	go func() {
		wg.Wait()

		close(resultados)

		fmt.Println("Canal resultados fechado.")
	}()

	// Simula o MAIN trabalhando por alguns segundos.
	go func() {
		time.Sleep(3 * time.Second)

		fmt.Println("MAIN: solicitando cancelamento!")
		cancel()
	}()

	// Recebe os resultados.
	for resultado := range resultados {
		fmt.Println("MAIN recebeu:", resultado)
	}

	fmt.Println("MAIN: Worker Pool terminou.")
}

func WorkerPoolContext(
	id int,
	ctx context.Context,
	jobs <-chan int,
	resultados chan<- string,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for {
		select {

		case <-ctx.Done():
			fmt.Printf("Worker %d recebeu cancelamento.\n", id)
			return

		case job, ok := <-jobs:

			if !ok {
				fmt.Printf("Worker %d: canal jobs fechado.\n", id)
				return
			}

			fmt.Printf("Worker %d recebeu Job %d\n", id, job)

			// Simula o processamento do Job.
			select {

			case <-ctx.Done():
				fmt.Printf(
					"Worker %d cancelado durante Job %d.\n",
					id,
					job,
				)
				return

			case <-time.After(1 * time.Second):
				// Job terminou normalmente.
			}

			// Envia o resultado somente se ainda não foi cancelado.
			select {

			case <-ctx.Done():
				fmt.Printf(
					"Worker %d cancelado antes de enviar Job %d.\n",
					id,
					job,
				)
				return

			case resultados <- fmt.Sprintf(
				"Worker %d concluiu Job %d",
				id,
				job,
			):
			}
		}
	}
}
