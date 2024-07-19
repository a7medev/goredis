package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/a7medev/goredis/config"
	"github.com/a7medev/goredis/resp"
	"github.com/a7medev/goredis/storage"
)

const BufferSize = 4096

type CommandHandler func(ctx *Context)

type Context struct {
	Conn

	Config *config.Config
	DB     *storage.Database

	Command string
	Args    []string

	FromMaster bool
}

func (s *Server) newContext(conn Conn, command string, args []string, fromMaster bool) *Context {
	return &Context{
		Conn:    conn,
		Config:  s.config,
		DB:      s.db,
		Command: command,
		Args:    args,
	}
}

type Server struct {
	listener net.Listener
	config   *config.Config
	db       *storage.Database
	commands map[string]CommandHandler
}

func NewServer(cfg *config.Config) *Server {
	return &Server{
		config:   cfg,
		commands: make(map[string]CommandHandler),
	}
}

// TODO: make the server exit gracefully.
func (s *Server) Start() {
	s.config.Mu.RLock()

	addr := fmt.Sprintf(":%v", s.config.Server.Port)
	ln, err := net.Listen("tcp", addr)

	if err != nil {
		log.Fatalln("Failed to listen on address", addr)
	}

	fmt.Println("Listening on", addr)

	s.listener = ln

	s.db = storage.NewDatabase()

	if s.config.Replication.Role == config.RoleModeSlave {
		go s.startReplication()
	}

	s.config.Mu.RUnlock()

	for {
		conn, err := s.listener.Accept()

		if err != nil {
			log.Printf("Error accepting connection %v: %v\n", conn.RemoteAddr(), err.Error())
			continue
		}

		go s.handleConn(NewNetConn(conn))
	}
}

func (s *Server) AddCommand(cmd string, handler CommandHandler) {
	s.commands[cmd] = handler
}

// parseCommand parses the recieved Redis command from the client.
// It reads the command, arguments, and the error if any.
func parseCommand(buf *bufio.Reader) (string, []string, error) {
	p := resp.NewParser(buf)

	cmdLen, err := p.NextArrayLength()

	if err != nil {
		return "", nil, err
	}

	cmd, err := p.NextBulkString()

	if err != nil {
		return "", nil, err
	}

	cmd = strings.ToUpper(cmd)

	argsLen := cmdLen - 1
	args := make([]string, argsLen)

	for i := range argsLen {
		arg, err := p.NextBulkString()

		if err != nil {
			return "", nil, err
		}

		args[i] = arg
	}

	return cmd, args, nil
}

func createCommand(cmd string, args ...string) *resp.Array {
	arr := resp.NewArray(resp.NewBulkString(cmd))

	for _, arg := range args {
		arr.Append(resp.NewBulkString(arg))
	}

	return arr
}

func (s *Server) handleConn(conn Conn) {
	defer conn.Close()

	fmt.Println("Connection from", conn.Addr())

	buf := conn.Reader()

	for {
		cmd, args, err := parseCommand(buf)

		if err == io.EOF {
			fmt.Println("Client closed connection", conn.Addr())
			return
		}

		ctx := s.newContext(conn, cmd, args, false)

		if err != nil {
			ctx.Reply(resp.NewSimpleError("ERR failed to parse command"))
			return
		}

		handler, ok := s.commands[cmd]

		if ok {
			handler(ctx)
		} else {
			msg := fmt.Sprintf("ERR unknown command '%v'", cmd)
			ctx.Reply(resp.NewSimpleError(msg))
		}
	}
}
