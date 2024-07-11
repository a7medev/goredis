package main

import (
	"flag"
	"fmt"

	"github.com/a7medev/goredis/commands"
	"github.com/a7medev/goredis/server"
)

func main() {
	port := flag.Uint("port", 6379, "Port to listen on")
	flag.Parse()

	addr := fmt.Sprintf(":%v", *port)
	s := server.NewServer(addr)

	s.AddCommand("PING", commands.Ping)
	s.AddCommand("ECHO", commands.Echo)
	s.AddCommand("SET", commands.Set)
	s.AddCommand("GET", commands.Get)
	s.AddCommand("DEL", commands.Del)

	s.Listen()
}
