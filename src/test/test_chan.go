package main

import (
	"fmt"
	"time"
)

func main() {
	c := make(chan (int))

	go func() {
		time.Sleep(time.Second * 1)
		<-c
	}()

	start := time.Now()
	c <- 100
	fmt.Printf("%v\n", time.Since(start))
}
