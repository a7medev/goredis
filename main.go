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

	cfg := config.NewConfig(port)

	if replicaOf != "" {
		masterHost, s, ok := strings.Cut(replicaOf, " ")

		if !ok {
			log.Fatal("Invalid replicaof argument")
		}

		masterPort, err := strconv.ParseUint(s, 10, 16)

		if err != nil {
			log.Fatal("Invalid replicaof argument", err)
		}

		cfg.Replication.Role = config.RoleModeSlave
		cfg.Replication.MasterHost = masterHost
		cfg.Replication.MasterPort = masterPort
	} else {
		cfg.Replication.Role = config.RoleModeMaster
		cfg.Replication.MasterReplID = config.RandomID(40)
		cfg.Replication.MasterReplOffset = 0
	}

	s := server.NewServer(cfg)

	s.AddCommand("PING", commands.Ping)
	s.AddCommand("ECHO", commands.Echo)
	s.AddCommand("SET", commands.Set).WithIsWrite(true)
	s.AddCommand("GET", commands.Get)
	s.AddCommand("DEL", commands.Del).WithIsWrite(true)
	s.AddCommand("INFO", commands.Info)
	s.AddCommand("REPLCONF", commands.ReplConf)
	s.AddCommand("PSYNC", commands.PSync)

	s.Start()
}
