package ctxjobcomp

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

		resultados <- Resultado{
			Job: job,
			Err: err,
		}
	}

	fmt.Printf("Worker %d: fila encerrada.\n", id)
}

func CtxJobsMain() {
	const totalJobs = 10
	const totalWorkers = 3

	jobs := make(chan int)
	resultados := make(chan Resultado)

	var wg sync.WaitGroup
	wg.Add(totalWorkers)

	for id := 1; id <= totalWorkers; id++ {
		go worker(id, jobs, resultados, &wg)
	}

	// Só fecha resultados depois que todos os Workers terminarem.
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
	falhaCriticaDetectada := false

	jobsEnviados := 0
	resultadosRecebidos := 0
	sucessos := 0
	falhas := 0

	for resultados != nil {
		select {
		case enviar <- jobParaEnviar:
			jobsEnviados++

			fmt.Printf(
				"MAIN entregou Job %d ao Worker.\n",
				jobParaEnviar,
			)

			proximoJob++

			if proximoJob <= totalJobs {
				jobParaEnviar = proximoJob
			} else {
				enviar = nil
				close(jobs)
				jobsFechado = true

				fmt.Println("MAIN: todos os Jobs foram enviados.")
			}

		case resultado, ok := <-resultados:
			if !ok {
				resultados = nil
				continue
			}

			resultadosRecebidos++

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

			if resultado.Job == 3 &&
				resultado.Err != nil &&
				!falhaCriticaDetectada {

				falhaCriticaDetectada = true
				enviar = nil

				fmt.Println(
					"MAIN: falha crítica; " +
						"não enviaremos novos Jobs.",
				)

				if !jobsFechado {
					close(jobs)
					jobsFechado = true
				}
			}
		}
	}

	jobsNaoEnviados := totalJobs - jobsEnviados

	fmt.Println("\n--- RESUMO DO LOTE ---")
	fmt.Printf("Jobs previstos:       %d\n", totalJobs)
	fmt.Printf("Jobs entregues:       %d\n", jobsEnviados)
	fmt.Printf("Jobs não enviados:    %d\n", jobsNaoEnviados)
	fmt.Printf("Resultados recebidos: %d\n", resultadosRecebidos)
	fmt.Printf("Sucessos:             %d\n", sucessos)
	fmt.Printf("Falhas:               %d\n", falhas)
}
