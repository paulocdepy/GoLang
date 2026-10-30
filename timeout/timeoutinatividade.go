package main

import (
	"fmt"
	"time"
)

func mainTout() {
	resultados := make(chan string)

	go func() {
		time.Sleep(1 * time.Second)
		resultados <- "Resultado 1"

		time.Sleep(1 * time.Second)
		resultados <- "Resultado 2"

		time.Sleep(1 * time.Second)
		resultados <- "Resultado 3"

		time.Sleep(1 * time.Second)
		resultados <- "Resultado 4"

		time.Sleep(1 * time.Second)
		resultados <- "Resultado 5"

		// time.Sleep(4 * time.Second)
		// resultados <- "Resultado 3"
	}()

	// UM único timeout para a operação inteira
	timeout := time.After(6 * time.Second)

	for {
		select {

		case <-timeout:
			fmt.Println("TIMEOUT TOTAL: operação ultrapassou 5 segundos.")
			return

		case resultado := <-resultados:
			fmt.Println("Recebi:", resultado)

			// case <-time.After(2 * time.Second):
			//	fmt.Println("TIMEOUT: fiquei 2 segundos sem receber nada.")
			//	return
		}
	}
}
