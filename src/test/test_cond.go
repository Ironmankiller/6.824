package main

import (
	"math/rand"
	"sync"
	"time"
)

func main() {
	var mutex sync.Mutex
	cond := sync.NewCond(&mutex)

	count := 0
	finish := 0

	for i := 0; i < 10; i++ {
		go func() {
			vote := requestVote()
			mutex.Lock()
			defer mutex.Unlock()
			if vote {
				count++
			}
			finish++
			cond.Signal()
		}()
	}

	mutex.Lock()
	for finish != 10 && count < 5 {
		cond.Wait()
	}
	println(finish)
	if count >= 5 {
		println("Be chosed!")
	} else {
		println("lost")
	}
	mutex.Unlock()
}

func requestVote() bool {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return rand.Int()%2 == 0
}
