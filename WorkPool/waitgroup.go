package main

import (
	"fmt"
	"sync"
	"time"
)

func worker(id int, wg *sync.WaitGroup, resultados chan<- string) {
	defer wg.Done()

	for job := 1; job <= 2; job++ {
		time.Sleep(500 * time.Millisecond)

		resultado := fmt.Sprintf(
			"Worker %d concluiu Job %d", id, job,
		)

		resultados <- resultado
	}

	fmt.Println("Worker", id, "terminou.")
}

// func waitGroup1() {
func main() {
	var wg sync.WaitGroup

	resultados := make(chan string)

	// Vamos iniciar 3 Workers.
	wg.Add(3)

	for i := 1; i <= 3; i++ {
		go worker(i, &wg, resultados)
	}

	// Esta goroutine aguarda todos os Workers.
	go func() {
		wg.Wait()
		time.Sleep(500 * time.Millisecond)
		close(resultados)
		fmt.Println("Canal resultados fechado.")
	}()

	// O MAIN rsecebe até o canal ser fechado.
	for resultado := range resultados {
		fmt.Println(resultado)
	}

	fmt.Println("MAIN : processamento encerrado.")
}

/*
func waitGroup2() {
	var wg sync.WaitGroup
	var wgFechamento sync.WaitGroup

	resultados := make(chan string)

	wg.Add(3)

	for i := 1; i <= 3; i++ {
		go worker(i, &wg, resultados)
	}

	wgFechamento.Add(1)

	go func() {
		defer wgFechamento.Done()

		wg.Wait()
		close(resultados)

		fmt.Println("Canal resultados fechado.")
	}()

	for resultado := range resultados {
		fmt.Println(resultado)
	}

	wgFechamento.Wait()

	fmt.Println("Exemplo WaitGroup 2 : processamento encerrado.")
}


func main() {
	waitGroup1()
	fmt.Println("---------------")
	//waitGroup2()
	//fmt.Println("WaitGroup 2 MAIN: processamento encerrado.")
}

*/
