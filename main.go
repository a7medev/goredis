package main

import (
	"flag"
	"log"
	"strconv"
	"strings"

	"github.com/a7medev/goredis/commands"
	"github.com/a7medev/goredis/config"
	"github.com/a7medev/goredis/server"
)

func main() {
	var port uint
	var replicaOf string

	flag.UintVar(&port, "port", 6379, "Port to listen on")
	flag.StringVar(&replicaOf, "replicaof", "", "Master server to replicate from as 'host port'")
	flag.Parse()

	configBuilder := config.NewConfigBuilder().
		WithPort(port).
		WithRole(config.RoleModeMaster)

	if replicaOf != "" {
		masterHost, s, ok := strings.Cut(replicaOf, " ")

		if !ok {
			log.Fatal("Invalid replicaof argument")
		}

		masterPort, err := strconv.ParseUint(s, 10, 16)

		if err != nil {
			log.Fatal("Invalid replicaof argument", err)
		}

		configBuilder.WithRole(config.RoleModeSlave).
			WithMasterHost(masterHost).
			WithMasterPort(masterPort)
	}

	s := server.NewServer(configBuilder.Build())

	s.AddCommand("PING", commands.Ping)
	s.AddCommand("ECHO", commands.Echo)
	s.AddCommand("SET", commands.Set)
	s.AddCommand("GET", commands.Get)
	s.AddCommand("DEL", commands.Del)
	s.AddCommand("INFO", commands.Info)

	s.Start()
}
