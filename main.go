package main

import (
	"github.com/a7medev/goredis/commands"
	"github.com/a7medev/goredis/server"
)

func main() {
	s := server.NewServer(":6379")

	s.AddCommand("PING", commands.Ping)
	s.AddCommand("ECHO", commands.Echo)
	s.AddCommand("SET", commands.Set)
	s.AddCommand("GET", commands.Get)

	s.Listen()
}
