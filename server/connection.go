package server

import (
	"bufio"
	"net"

	"github.com/a7medev/goredis/resp"
)

type Conn interface {
	Reply(reply resp.Encodable) error
	Reader() *bufio.Reader
	Close() error
	Addr() string
}

type NetConn struct {
	conn net.Conn
}

func NewNetConn(conn net.Conn) *NetConn {
	return &NetConn{conn: conn}
}

func (c *NetConn) Reply(reply resp.Encodable) error {
	b := []byte(reply.Encode())

	_, err := c.conn.Write(b)

	return err
}

func (c *NetConn) Reader() *bufio.Reader {
	return bufio.NewReader(c.conn)
}

func (c *NetConn) Close() error {
	return c.conn.Close()
}

func (c *NetConn) Addr() string {
	return c.conn.RemoteAddr().String()
}
