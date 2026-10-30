package ctxcancel

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Resultado struct {
	Job int
	Err error
}

func processar(ctx context.Context, job int) error {
	fmt.Printf("Processando Job %d...\n", job)

	// Simula uma operação que pode ser interrompida.
	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-time.After(700 * time.Millisecond):
	}

	// Simula uma falha crítica de infraestrutura.
	if job == 3 {
		return fmt.Errorf("banco de dados indisponível")
	}

	return nil
}

func worker(
	id int,
	ctx context.Context,
	jobs <-chan int,
	resultados chan<- Resultado,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Worker %d: cancelamento recebido.\n", id)
			return

		case job, ok := <-jobs:
			if !ok {
				fmt.Printf("Worker %d: fila encerrada.\n", id)
				return
			}

			err := processar(ctx, job)

			// Entrega o resultado antes de decidir encerrar.
			select {
			case resultados <- Resultado{Job: job, Err: err}:
			case <-ctx.Done():
				return
			}

			if err != nil {
				fmt.Printf("Worker %d: erro no Job %d: %v\n",
					id, job, err)

				// Erro crítico: este Worker encerra.
				// O main coordenará o cancelamento geral.
				if job == 3 {
					return
				}

				if ctx.Err() != nil {
					return
				}
			}
		}
	}
}

func CallCtxCancel() {
	const totalJobs = 10
	const totalWorkers = 3

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan int)
	resultados := make(chan Resultado)

	var wg sync.WaitGroup
	wg.Add(totalWorkers)

	for id := 1; id <= totalWorkers; id++ {
		go worker(id, ctx, jobs, resultados, &wg)
	}

	// O produtor é o único responsável por fechar jobs.
	go func() {
		defer close(jobs)

		for job := 1; job <= totalJobs; job++ {
			select {
			case <-ctx.Done():
				fmt.Println("Produtor: cancelado; não envia mais Jobs.")
				return

			case jobs <- job:
				fmt.Printf("Produtor enviou Job %d\n", job)
			}
		}
	}()

	// Fecha resultados somente quando todos os Workers terminarem.
	go func() {
		wg.Wait()
		close(resultados)
	}()

	sucessos := 0
	falhas := 0
	cancelamentoIniciado := false

	// Continua consumindo até resultados ser fechado.
	for resultado := range resultados {
		if resultado.Err != nil {
			falhas++

			fmt.Printf("MAIN: Job %d falhou: %v\n",
				resultado.Job, resultado.Err)

			if resultado.Job == 3 && !cancelamentoIniciado {
				cancelamentoIniciado = true
				fmt.Println("MAIN: iniciando cancelamento geral.")
				cancel()
			}

			continue
		}

		sucessos++
		fmt.Printf("MAIN: Job %d concluído.\n", resultado.Job)
	}

	fmt.Printf(
		"\nEncerramento: %d sucessos, %d falhas.\n",
		sucessos, falhas,
	)
}
