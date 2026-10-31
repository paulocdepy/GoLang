package datarace

import (
	"fmt"
	"sync"
)

func MainDataRace() {
	const totalGoroutines = 100
	const incrementos = 10000

	contador := 0

	var wg sync.WaitGroup
	var mu sync.Mutex

	wg.Add(totalGoroutines)

	for i := 0; i < totalGoroutines; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < incrementos; j++ {

				mu.Lock()

				contador++

				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	fmt.Println("Resultado esperado:", totalGoroutines*incrementos)
	fmt.Println("Resultado obtido:  ", contador)
}
