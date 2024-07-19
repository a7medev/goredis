package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/a7medev/goredis/config"
	"github.com/a7medev/goredis/resp"
	"github.com/a7medev/goredis/storage"
)

const BufferSize = 4096

type CommandHandler func(ctx *Context)

type Context struct {
	Config  *config.Config
	Conn    net.Conn
	DB      *storage.Database
	Command string
	Args    []string
}

func newContext(config *config.Config, conn net.Conn, db *storage.Database, command string, args []string) *Context {
	return &Context{
		Config:  config,
		Conn:    conn,
		DB:      db,
		Command: command,
		Args:    args,
	}
}

func (c *Context) Reply(reply resp.Encodable) error {
	s := strings.NewReader(reply.Encode())

	_, err := io.Copy(c.Conn, s)

	return err
}

type Server struct {
	Listener net.Listener
	Config   *config.Config
	commands map[string]CommandHandler
}

func NewServer(cfg *config.Config) *Server {
	return &Server{
		Config:   cfg,
		commands: make(map[string]CommandHandler),
	}
}

// TODO: make the server exit gracefully.
func (s *Server) Start() {
	s.Config.Mu.RLock()

	addr := fmt.Sprintf(":%v", s.Config.Server.Port)
	ln, err := net.Listen("tcp", addr)

	if err != nil {
		log.Fatalln("Failed to bind to listen on address", addr)
	}

	fmt.Println("Listening on", addr)

	s.Listener = ln

	db := storage.NewDatabase()

	if s.Config.Replication.Role == config.RoleModeSlave {
		go s.startReplication()
	}

	s.Config.Mu.RUnlock()

	for {
		conn, err := s.Listener.Accept()

		if err != nil {
			log.Printf("Error accepting connection %v: %v\n", conn.RemoteAddr(), err.Error())
			continue
		}

		go s.handleConn(conn, db)
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

func (s *Server) handleConn(conn net.Conn, db *storage.Database) {
	defer conn.Close()

	fmt.Println("Connection from", conn.RemoteAddr())

	buf := bufio.NewReader(conn)

	for {
		cmd, args, err := parseCommand(buf)

		if err == io.EOF {
			fmt.Println("Client closed connection", conn.RemoteAddr())
			return
		}

		ctx := newContext(s.Config, conn, db, cmd, args)

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

// startReplication connects to the master server and starts the replication process.
// It sends the PING command to the master server to check if it's alive.
// It then sends the REPLCONF listening-port <port> and REPLCONF capa eof capa psync2 commands.
func (s *Server) startReplication() {
	s.Config.Mu.Lock()
	defer s.Config.Mu.Unlock()

	// Connect to master
	addr := fmt.Sprintf("%v:%v", s.Config.Replication.MasterHost, s.Config.Replication.MasterPort)
	conn, err := net.Dial("tcp", addr)

	if err != nil {
		fmt.Println("Failed to connect to master", addr)
		return
	}

	defer conn.Close()

	fmt.Println("Connected to master", addr)

	// PING master
	ping := resp.NewArray(resp.NewBulkString("PING"))
	_, err = conn.Write([]byte(ping.Encode()))

	if err != nil {
		fmt.Println("Failed to PING master")
		return
	}

	buf := bufio.NewReader(conn)
	p := resp.NewParser(buf)

	pong, err := p.NextSimpleString()

	if err != nil {
		fmt.Println("Failed to parse PONG from master")
		return
	}

	if pong != "PONG" {
		fmt.Println("Master didn't reply with PONG but rather with", pong)
		return
	}

	fmt.Println("Master replied with PONG, starting replication")

	// REPLCONF listening-port <port>
	// REPLCONF capa eof capa psync2

	replconf := resp.NewBulkString("REPLCONF")
	listiningPort := resp.NewBulkString("listening-port")
	port := resp.NewBulkString(strconv.Itoa(int(s.Config.Server.Port)))
	cmd := resp.NewArray(replconf, listiningPort, port)

	_, err = conn.Write([]byte(cmd.Encode()))

	if err != nil {
		fmt.Println("Failed to send REPLCONF listening-port to master")
		return
	}

	ok, err := p.NextSimpleString()
	if err != nil {
		fmt.Println("Failed to parse result from master")
		return
	}

	if ok != "OK" {
		fmt.Println("Master didn't reply with OK but rather with", ok)
		return
	}

	fmt.Println("Master replied with OK to REPLCONF listening-port")

	capa := resp.NewBulkString("capa")
	eof := resp.NewBulkString("eof")
	psync2 := resp.NewBulkString("psync2")
	cmd = resp.NewArray(replconf, capa, eof, capa, psync2)

	_, err = conn.Write([]byte(cmd.Encode()))

	if err != nil {
		fmt.Println("Failed to send REPLCONF capa psync2 to master")
		return
	}

	ok, err = p.NextSimpleString()

	if err != nil {
		fmt.Println("Failed to parse result from master")
		return
	}

	if ok != "OK" {
		fmt.Println("Master didn't reply with OK but rather with", ok)
		return
	}

	fmt.Println("Master replied with OK to REPLCONF capa psync2")

	// PSYNC <replicationId> <offset>

	psync := resp.NewBulkString("PSYNC")
	replId := resp.NewBulkString(s.Config.Replication.MasterReplID)
	offset := resp.NewBulkString(strconv.Itoa(s.Config.Replication.MasterReplOffset))
	cmd = resp.NewArray(psync, replId, offset)

	_, err = conn.Write([]byte(cmd.Encode()))

	if err != nil {
		fmt.Println("Failed to send PSYNC to master")
		return
	}

	// TODO: Read PSYNC reply from master
	// TODO: Read RDB from master and load it into the database
	// Should do so after implementing RDB encoding/decoding.

	result, err := p.NextSimpleString()

	if err != nil {
		fmt.Println("Failed to parse PSYNC result from master")
		return
	}

	fmt.Println("Master replied with", result)

	syncArgs := strings.Split(result, " ")

	switch syncArgs[0] {
	case "CONTINUE":
		fmt.Println("Master replied with CONTINUE, partial sync will follow")
	case "FULLRESYNC":
		fmt.Println("Master requested a full sync")

		replId := syncArgs[1]
		offset, err := strconv.Atoi(syncArgs[2])

		if err != nil {
			fmt.Println("Failed to parse offset from master")
			return
		}

		fmt.Println("Master replId:", replId, "offset:", offset)

		s.Config.Replication.MasterReplID = replId
		s.Config.Replication.MasterReplOffset = offset

		// Read RDB file from server
		// TODO: update the server configuration with the RDB file.
		// TODO: Adjust the buffer size as well to account for larger RDB files if needed.
		// _, err = conn.Read(buf)

		// if err != nil {
		// 	fmt.Println("Failed to read RDB from master")
		// 	return
		// }

		// fmt.Println("Received RDB from master")
	default:
		fmt.Println("Unknown PSYNC result from master")
	}
}
