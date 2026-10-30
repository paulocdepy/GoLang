package ctxcancel2

import (
	"fmt"
	"sync"
	"time"
)

type Resultado struct {
	Job int
	Err error
}

func processar(job int) error {
	fmt.Printf("Processando Job %d...\n", job)

	// Simula uma operação que não será interrompida
	// apenas porque outro Job apresentou uma falha.
	time.Sleep(700 * time.Millisecond)

	if job == 3 {
		return fmt.Errorf("banco de dados indisponível")
	}

	return nil
}

func worker(
	id int,
	jobs <-chan int,
	resultados chan<- Resultado,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for job := range jobs {
		err := processar(job)

		// O Worker entrega o resultado antes de
		// buscar outro Job.
		resultados <- Resultado{
			Job: job,
			Err: err,
		}
	}

	fmt.Printf("Worker  %d: fila encerrada.\n", id)
}

func CtxMainCancel() {
	const totalJobs = 10
	const totalWorkers = 3

	jobs := make(chan int)
	resultados := make(chan Resultado)

	var wg sync.WaitGroup
	wg.Add(totalWorkers)

	for id := 1; id <= totalWorkers; id++ {
		go worker(id, jobs, resultados, &wg)
	}

	// Fecha resultados somente quando todos os
	// Workers terminarem.
	go func() {
		wg.Wait()
		close(resultados)
	}()

	proximoJob := 1
	var jobParaEnviar int
	var enviar chan<- int

	if proximoJob <= totalJobs {
		jobParaEnviar = proximoJob
		enviar = jobs
	}

	jobsFechado := false
	cancelamentoIniciado := false

	sucessos := 0
	falhas := 0

	// O main distribui Jobs e recebe resultados.
	for resultados != nil {
		select {
		case enviar <- jobParaEnviar:
			fmt.Printf("MAIN enviou Job %d\n", jobParaEnviar)

			proximoJob++

			if proximoJob <= totalJobs {
				jobParaEnviar = proximoJob
			} else {
				// Todos os Jobs foram enviados.
				enviar = nil
				close(jobs)
				jobsFechado = true

				fmt.Println(
					"MAIN: todos os Jobs foram enviados.",
				)
			}

		case resultado, ok := <-resultados:
			if !ok {
				resultados = nil
				continue
			}

			if resultado.Err != nil {
				falhas++

				fmt.Printf(
					"MAIN: Job %d falhou: %v\n",
					resultado.Job,
					resultado.Err,
				)
			} else {
				sucessos++

				fmt.Printf(
					"MAIN: Job %d concluído.\n",
					resultado.Job,
				)
			}

			// A falha crítica impede novas distribuições
			// depois que o main recebe esse resultado.
			if resultado.Job == 3 &&
				resultado.Err != nil &&
				!cancelamentoIniciado {

				cancelamentoIniciado = true

				fmt.Println(
					"MAIN: falha crítica; " +
						"não enviaremos novos Jobs.",
				)

				enviar = nil

				if !jobsFechado {
					close(jobs)
					jobsFechado = true
				}
			}
		}
	}

	fmt.Printf(
		"\nEncerramento: %d sucessos, %d falhas.\n",
		sucessos,
		falhas,
	)
}
