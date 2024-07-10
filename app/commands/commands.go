package commands

import (
	"fmt"

	"github.com/a7medev/goredis/app/resp"
	"github.com/a7medev/goredis/app/server"
	"github.com/a7medev/goredis/app/storage"
)

func Ping(c *server.Connection, db *storage.Database, p *resp.Parser, args int) {
	pong := resp.NewSimpleString("PONG")
	c.Reply(pong)
}

func Echo(c *server.Connection, db *storage.Database, p *resp.Parser, args int) {
	p.NextType()
	msg, err := p.NextBulkString()

	if err != nil {
		fmt.Println("Error parsing command:", err.Error())
		return
	}

	result := resp.NewBulkString(msg)

	c.Reply(result)
}

func Set(c *server.Connection, db *storage.Database, p *resp.Parser, args int) {
	p.NextType()
	key, err := p.NextBulkString()

	if err != nil {
		fmt.Println("Error parsing key:", err.Error())
		return
	}

	p.NextType()
	value, err := p.NextBulkString()

	if err != nil {
		fmt.Println("Error parsing value:", err.Error())
		return
	}

	db.Set(key, value, storage.NeverExpires)

	ok := resp.NewSimpleString("OK")

	c.Reply(ok)
}

func Get(c *server.Connection, db *storage.Database, p *resp.Parser, args int) {
	p.NextType()
	key, err := p.NextBulkString()

	if err != nil {
		fmt.Println("Error parsing key: ", err.Error())
		return
	}

	value, ok := db.Get(key)

	if !ok {
		null := resp.NewNullBulkString()

		c.Reply(null)
	} else {
		result := resp.NewBulkString(value)

		c.Reply(result)
	}
}
