package datarace

import (
	"fmt"
	"sync"
	"time"
)

func MainDataRace() {
	var mu sync.Mutex
	var wg sync.WaitGroup

	tentou := make(chan struct{})

	// MAIN adquire o Mutex.
	mu.Lock()

	fmt.Println("MAIN: adquiriu o Mutex.")

	wg.Add(1)

	go func() {
		defer wg.Done()

		fmt.Println("Worker: tentando adquirir o Mutex...")

		// Avisa ao MAIN que está prestes a tentar.
		close(tentou)

		// Aqui ficará bloqueada enquanto MAIN
		// mantiver o Mutex ocupado.
		mu.Lock()
		defer mu.Unlock()

		fmt.Println("Worker: conseguiu adquirir o Mutex!")

		time.Sleep(500 * time.Millisecond)

		fmt.Println("Worker: terminou e liberou o Mutex.")
	}()

	// Espera o Worker anunciar sua tentativa.
	<-tentou

	fmt.Println("MAIN: mantendo o Mutex bloqueado por 2 segundos.")

	time.Sleep(2 * time.Second)

	fmt.Println("MAIN: liberando o Mutex.")

	mu.Unlock()

	wg.Wait()

	fmt.Println("MAIN: todas as goroutines terminaram.")
}
