package main

import (
	"context"
	"fmt"
	"time"
)

func timeFinish() {
	resultado := make(chan string)

	go func() {
		time.Sleep(7 * time.Second)
		resultado <- "Operação terminou!"
	}()

	timer := time.NewTimer(5 * time.Second)

	select {

	case msg := <-resultado:
		fmt.Println(msg)

		if timer.Stop() {
			fmt.Println("Timer foi desarmado.")
		}

	case <-timer.C:
		fmt.Println("TIMEOUT!")

	}

	fmt.Println("Main terminou.")
}

func timeReset() {
	timer := time.NewTimer(2 * time.Second)
	fmt.Println("Timer armado.")
	time.Sleep(3 * time.Second)
	timer.Reset(2 * time.Second)
	fmt.Println("Timer rearmado.")
	<-timer.C
	fmt.Println("Disparo recebido!")
}

// Timer de inatividade com Reset()
func timeResetSelect() {
	resultados := make(chan string)

	// Simula uma operação que produz resultados
	go func() {
		time.Sleep(1 * time.Second)
		resultados <- "Resultado 1"

		time.Sleep(1 * time.Second)
		resultados <- "Resultado 2"

		time.Sleep(4 * time.Second)
		resultados <- "Resultado 3"
	}()

	// Timer de inatividade: 3 segundos
	timer := time.NewTimer(3 * time.Second)

	for {
		select {

		case resultado := <-resultados:
			fmt.Println("Recebi:", resultado)

			// Reinicia o prazo de inatividade
			timer.Reset(3 * time.Second)

		case <-timer.C:
			fmt.Println("TIMEOUT: 3 segundos sem receber resultados.")
			return
		}
	}
}

func timeTimeOut() {
	resultado := make(chan string)

	go func() {
		time.Sleep(2 * time.Second)
		resultado <- "Processamento concluído!"
	}()

	timer := time.NewTimer(5 * time.Second)

	select {

	case msg := <-resultado:
		fmt.Println(msg)

		if timer.Stop() {
			fmt.Println("Timer desarmado.")
		}

	case <-timer.C:
		fmt.Println("TIMEOUT!")
	}

	fmt.Println("TimeOut terminou.")
}

func timeTimeOutFor() {
	resultados := make(chan string)

	go func() {
		time.Sleep(1 * time.Second)
		resultados <- "Resultado 1"

		time.Sleep(2 * time.Second)
		resultados <- "Resultado 2"

		time.Sleep(5 * time.Second)
		resultados <- "Resultado 3"
	}()

	timer := time.NewTimer(3 * time.Second)

	for {
		select {

		case resultado := <-resultados:
			fmt.Println("Recebi:", resultado)

			timer.Reset(3 * time.Second)

		case <-timer.C:
			fmt.Println("TIMEOUT: 3 segundos sem resultado.")
			return
		}
	}
}

func timeComContext() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultados := make(chan string)

	// Simula uma operação trabalhando
	go func() {
		time.Sleep(1 * time.Second)
		resultados <- "Resultado 1"

		time.Sleep(2 * time.Second)
		resultados <- "Resultado 2"

		time.Sleep(5 * time.Second)
		resultados <- "Resultado 3"
	}()

	timer := time.NewTimer(3 * time.Second)

	for {
		select {

		case resultado := <-resultados:
			fmt.Println("Recebi:", resultado)

			// Recebi atividade:
			// renovo o prazo de inatividade.
			timer.Reset(3 * time.Second)

		case <-timer.C:
			fmt.Println("TIMEOUT: 3 segundos sem resultado.")
			return

		case <-ctx.Done():
			fmt.Println("CANCELAMENTO: operação cancelada.")
			return
		}
	}
}

func workerTimer(ctx context.Context) {
	for {
		select {

		case <-ctx.Done():
			fmt.Println("Worker recebeu cancelamento.")
			return

		default:
			fmt.Println("Worker trabalhando...")
			time.Sleep(1 * time.Second)
		}
	}
}

func timeContWorker() {
	ctx, cancel := context.WithCancel(context.Background())

	go workerTimer(ctx)

	time.Sleep(4 * time.Second)

	fmt.Println("MAIN: solicitando cancelamento...")
	cancel()

	time.Sleep(1 * time.Second)

	fmt.Println("MAIN: terminou.")
}

func mainTest() {
	resultados := make(chan string)

	go func() {
		time.Sleep(1 * time.Second)
		resultados <- "Resultado 1"

		time.Sleep(1 * time.Second)
		resultados <- "Resultado 2"

		time.Sleep(4 * time.Second)
		resultados <- "Resultado 3"
	}()

	timer := time.NewTimer(2 * time.Second)

	for {
		select {

		case resultado := <-resultados:
			fmt.Println("Recebi:", resultado)

		case <-timer.C:
			fmt.Println("TIMEOUT!")
			fmt.Println("- ...-Time Finish")
			timeFinish()
			fmt.Println("- ...-Timer Reset..")
			timeReset()
			fmt.Println("- ...-Timer Inatividade..")
			timeResetSelect()
			fmt.Println("- ...-Timer TimeOut.....")
			timeTimeOut()

			fmt.Println("- ...-Timer TimeOut com For .....")
			timeTimeOutFor()

			fmt.Println("- ...-Timer + Context.....")
			timeComContext()

			fmt.Println("- ...-Timer + Context + Worker.....")
			timeContWorker()

			fmt.Println("-------------------------")

			return
		}
	}
}
