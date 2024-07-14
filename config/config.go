package config

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
)

type Config struct {
	Server      ServerConfig
	Replication ReplicationConfig
	Mu          *sync.RWMutex
}

type ServerConfig struct {
	Port uint
}

type RoleMode string

const (
	RoleModeMaster RoleMode = "master"
	RoleModeSlave  RoleMode = "slave"
)

type ReplicationConfig struct {
	Role             RoleMode
	MasterHost       string
	MasterPort       uint64
	MasterReplID     string
	MasterReplOffset int
	ConnectedSlaves  uint
}

// entry converts a config entry to a string in the format used in the INFO command.
func entry(name string, value any) string {
	return fmt.Sprintf("%s:%v\n", name, value)
}

func (c *ServerConfig) String() string {
	b := strings.Builder{}

	b.WriteString("# Server\n")
	b.WriteString(entry("tcp_port", c.Port))
	b.WriteByte('\n')

	return b.String()
}

func (c *ReplicationConfig) String() string {
	b := strings.Builder{}

	b.WriteString("# Replication\n")

	b.WriteString(entry("role", c.Role))
	b.WriteString(entry("connected_slaves", c.ConnectedSlaves))
	b.WriteString(entry("master_replid", c.MasterReplID))
	b.WriteString(entry("master_repl_offset", c.MasterReplOffset))

	b.WriteByte('\n')

	return b.String()
}

func NewConfig(port uint) *Config {
	return &Config{
		Mu:     new(sync.RWMutex),
		Server: ServerConfig{Port: port},
		Replication: ReplicationConfig{
			Role:             RoleModeMaster,
			MasterReplID:     "?",
			MasterReplOffset: -1,
		},
	}
}

// RandomID generates a hexadecimal random ID with specified characters length to be used as a master replication ID.
func RandomID(length int) string {
	hexadigits := "0123456789abcdef"
	n := len(hexadigits)

	b := make([]byte, length)
	for i := range b {
		b[i] = hexadigits[rand.Intn(n)]
	}

	return string(b)
}
