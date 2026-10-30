package main

import (
	"fmt"

	"pools/datarace"
)

func main() {
	//fmt.Println("- ...-Worker Pool + WaitGroup.....")
	//waitgroup.TimeWorkerPool()

	//fmt.Println("- ...-Worker Pool + Context + WaitGroup.....")
	//waitgroup.WorkerPoolComContext()

	//fmt.Println("- ...-Result Wait Group .....")
	//waitgroup.CallResultWaitGr()

	//fmt.Println("- ...-Encerramento Gracioso....")
	//waitgroup.CallEncerramentoGracioso()

	//fmt.Println("- ...-WorkPool com Erro......")
	//waitgroup.CallWorkPoolErr()
	//fmt.Println("- ...-Context con Cancell WorkPool .....")
	//ctxcancel.CallCtxCancel()

	//fmt.Println("- ...-Context Main Cancell .....")
	//ctxcancel2.CtxMainCancel()

	//fmt.Println("- ...-Context Jobs Complete .....")
	//ctxjobcomp.CtxJobsMain()

	fmt.Println("- ...Data Race Study.....")
	datarace.MainDataRace()
}
