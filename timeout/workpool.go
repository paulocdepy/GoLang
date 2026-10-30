package main

import (
	"context"
	"fmt"
	"time"
)

func worker(ctx context.Context, jobs <-chan int, resultados chan<- string) {
	for {
		select {

		case <-ctx.Done():
			fmt.Println("Worker: cancelado.")
			return

		case job, ok := <-jobs:

			if !ok {
				//fmt.Println("Worker: canal de jobs fechado.")
				return
			}

			fmt.Println("Worker recebeu Job", job)
			time.Sleep(1 * time.Second)
			// Simula o processamento do Job.
			select {

			case <-ctx.Done():
				fmt.Println("Worker: cancelado durante o Job", job)
				return

			case <-time.After(1 * time.Second):
				resultados <- fmt.Sprintf("Job %d concluído", job)
			}
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan int, 4)
	resultados := make(chan string, 4)

	go worker(ctx, jobs, resultados)

	// Envia alguns Jobs
	go func() {
		for i := 1; i <= 10; i++ {
			select {
			case jobs <- i:
				fmt.Println("MAIN enviou Job", i)
				time.Sleep(100 * time.Millisecond)

			case <-ctx.Done():
				fmt.Println("Produtor: cancelado.")
				return
			}
		}

		close(jobs)
	}()

	// Recebe resultados
	for {
		select {

		case resultado := <-resultados:
			fmt.Println("MAIN recebeu:", resultado)

		case <-time.After(5 * time.Second):
			fmt.Println("MAIN: timeout.")
			cancel()
			return
		}
	}
}
