package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
)

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Calculater int

func (*Calculater) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (*Calculater) Divide(args *Args, reply *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")

	}
	reply.Quo = args.A / args.B
	reply.Rem = args.A % args.B

	return nil
}

func goRpcServer() {
	rpc.Register(new(Calculater)) // 注册rpc服务
	rpc.HandleHTTP()

	lis, err := net.Listen("tcp4", ":2345")
	if err != nil {
		log.Fatalln("fatal error: ", err)
	}

	fmt.Fprintf(os.Stdout, "%s\n", "start connection")

	http.Serve(lis, nil)
}

func jsonRpcServer() {
	rpc.Register(new(Calculater)) // 注册rpc服务

	lis, err := net.Listen("tcp4", ":1234")
	if err != nil {
		log.Fatalln("fatal error: ", err)
	}

	fmt.Fprintf(os.Stdout, "%s\n", "start connection")

	for {
		conn, err := lis.Accept() // 接收客户端连接请求
		if err != nil {
			continue
		}
		go func(conn net.Conn) { // 并发处理客户端请求
			fmt.Fprintf(os.Stdout, "%s,my add:%v peer add %v\n", "new client in coming", conn.LocalAddr(), conn.RemoteAddr())
			jsonrpc.ServeConn(conn)
		}(conn)
	}
}

func main() {
	go jsonRpcServer()
	goRpcServer()
}
