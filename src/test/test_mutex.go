package main

import (
	"sync"
)

func main() {
	var a int = int(0)
	var wg sync.WaitGroup

	var lock sync.Mutex

	for i := 0; i < 100000; i++ {
		wg.Add(1)
		go func() {
			lock.Lock()
			defer lock.Unlock()
			a++
			wg.Done()
		}()
	}

	wg.Wait()

	println(a)
}
