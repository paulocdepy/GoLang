package main

import (
	"fmt"
	"time"
)

func main() {
	chA := make(chan string)
	chB := make(chan string)

	go func() {
		time.Sleep(3 * time.Second)
		chA <- "Mensagem do Channel A"
	}()
	go func() {
		time.Sleep(1 * time.Second)
		chB <- "Mensagem do Channel B"
	}()

	fmt.Println("Main entrou no select...")

	for i := 0; i < 2; i++ {
		select {

		case msg := <-chA:
			fmt.Println("Recebi:", msg)

		case msg := <-chB:
			fmt.Println("Recebi:", msg)
		}
	}

	fmt.Println("Main terminou.")
	channelFechado()
	fmt.Println("----------------Time After")
	timeAfter()
}

func channelFechado() {
	ch := make(chan int)

	close(ch)

	select {
	case valor := <-ch:
		fmt.Println("Recebi:", valor)

	default:
		fmt.Println("Default")
	}
}

func timeAfter() {
	ch := make(chan string)

	go func() {
		time.Sleep(2 * time.Second)
		ch <- "Resposta chegou!"
	}()

	select {

	case msg := <-ch:
		fmt.Println("Recebi:", msg)

	case <-time.After(3 * time.Second):
		fmt.Println("Timeout!")
	}

	fmt.Println("Main terminou.")
}
