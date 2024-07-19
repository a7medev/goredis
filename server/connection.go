package server

import (
	"bufio"
	"net"
	"sync"

	"github.com/a7medev/goredis/resp"
)

type Conn interface {
	Reply(reply resp.Encodable) error
	Reader() *bufio.Reader
	Close() error
	Addr() string
}

type NetConn struct {
	conn    net.Conn
	buf     *bufio.Reader
	bufOnce sync.Once
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
	c.bufOnce.Do(func() {
		c.buf = bufio.NewReader(c.conn)
	})

	return c.buf
}

func (c *NetConn) Close() error {
	return c.conn.Close()
}

func (c *NetConn) Addr() string {
	return c.conn.RemoteAddr().String()
}
