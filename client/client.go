package main

import (
	"log"
	"net"
	"time"

	pb "github.com/zballs/goPBFT/types"
)

func main() {
	creatClient("127.0.0.1:8080")
}

func creatClient(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Panic("Tcp connect error")
	}
	log.Print("Client start listen on ", addr)
	writeMessage("127.0.0.1:8090")
	acceptMessage(ln)
}

func acceptMessage(ln net.Listener) {
	log.Print("Client start get message")
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Panic(err)
		}
		rep := &pb.Reply{}
		err = pb.ReadMessage(conn, rep)
		if err != nil {
			log.Panic(err)
		}
		log.Print("After read, the Reply is ", rep)
	}
}

func writeMessage(addr string) {
	op := new(pb.Operation)
	op.Value = 10
	t := time.Now().String()
	log.Print("Creat a client request")
	req := pb.ToRequestClient(op, t, addr)
	log.Print("Start to write message to ", addr)
	pb.WriteMessage(addr, req)
}
