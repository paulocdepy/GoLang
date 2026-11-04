package main

import (
	"fmt"
	"sync"
	"time"
)

type Catalogo struct {
	mu       sync.RWMutex
	produtos map[int]string
}

func (c *Catalogo) consultar(
	id int,
	prontos chan<- struct{},
	liberar <-chan struct{},
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	c.mu.RLock()
	defer c.mu.RUnlock()

	fmt.Printf("Leitor %d: iniciou leitura.\n", id)

	// Avisa que já adquiriu RLock.
	prontos <- struct{}{}

	// Mantém a leitura aberta para nossa demonstração.
	<-liberar

	produto := c.produtos[1]

	fmt.Printf("Leitor %d: consultou %s.\n", id, produto)
}

func (c *Catalogo) atualizar(
	wg *sync.WaitGroup,
	tentando chan<- struct{},
) {
	defer wg.Done()

	fmt.Println("Escritor: tentando obter Lock...")

	tentando <- struct{}{}

	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Println("Escritor: conseguiu acesso exclusivo.")

	c.produtos[1] = "Notebook atualizado"

	fmt.Println("Escritor: produto atualizado.")
}

func main() {
	catalogo := Catalogo{
		produtos: map[int]string{
			1: "Notebook",
		},
	}

	var wg sync.WaitGroup

	prontos := make(chan struct{}, 2)
	liberar := make(chan struct{})
	tentando := make(chan struct{})

	wg.Add(3)

	// Inicia dois leitores.
	go catalogo.consultar(1, prontos, liberar, &wg)
	go catalogo.consultar(2, prontos, liberar, &wg)

	// Aguarda os dois leitores adquirirem RLock.
	<-prontos
	<-prontos

	// Inicia o escritor.
	go catalogo.atualizar(&wg, tentando)

	// Espera o escritor anunciar sua tentativa.
	<-tentando

	// Dá oportunidade para o escritor tentar Lock.
	time.Sleep(300 * time.Millisecond)

	fmt.Println("MAIN: liberando os leitores.")

	close(liberar)

	wg.Wait()

	fmt.Println("MAIN: processamento concluído.")
}