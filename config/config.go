package config

import (
	"fmt"
	"strings"
)

type Config struct {
	Server      ServerConfig
	Replication ReplicationConfig
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
	Role       RoleMode
	MasterHost string
	MasterPort uint64
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
	b.WriteString(entry("master_host", c.MasterHost))
	b.WriteString(entry("master_port", c.MasterPort))

	b.WriteByte('\n')

	return b.String()
}

type ConfigBuilder struct {
	config *Config
}

func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		config: &Config{Replication: ReplicationConfig{Role: RoleModeMaster}},
	}
}

func (b *ConfigBuilder) WithPort(port uint) *ConfigBuilder {
	b.config.Server.Port = port
	return b
}

func (b *ConfigBuilder) WithRole(role RoleMode) *ConfigBuilder {
	b.config.Replication.Role = role
	return b
}

func (b *ConfigBuilder) WithMasterHost(host string) *ConfigBuilder {
	b.config.Replication.MasterHost = host
	return b
}

func (b *ConfigBuilder) WithMasterPort(port uint64) *ConfigBuilder {
	b.config.Replication.MasterPort = port
	return b
}

func (b *ConfigBuilder) Build() *Config {
	return b.config
}
