package main

import (
	"fmt"
	"log"
	"net/rpc"
)

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

const serverAddress string = "192.168.153.130"

func main() {

	client, err := jsonrpc.DialHTTP("tcp", serverAddress+":1234")
	client, err := rpc.DialHTTP("tcp", serverAddress+":2345")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// Synchronous call
	args := &Args{58, 8}
	var reply Quotient
	err = client.Call("Calculater.Divide", args, &reply)
	if err != nil {
		log.Fatal("Calculater error:", err)
	}
	fmt.Printf("Calculater: %d/%d=%d...%d\n", args.A, args.B, reply.Quo, reply.Rem)
}
