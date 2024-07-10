package commands

import (
	"fmt"
	"strconv"
	"time"

	"github.com/a7medev/goredis/resp"
	"github.com/a7medev/goredis/server"
	"github.com/a7medev/goredis/storage"
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

	expiry := storage.NeverExpires

	if args == 4 {
		p.NextType()
		subCmd, err := p.NextBulkString()

		if err != nil {
			fmt.Println("Error parsing sub-command:", err.Error())
			return
		}

		switch subCmd {
		case "PX":
			p.NextType()
			expiryStr, err := p.NextBulkString()

			if err != nil {
				fmt.Println("Error parsing expiry:", err.Error())
				return
			}

			expiryMs, err := strconv.Atoi(expiryStr)

			if err != nil {
				fmt.Println("Error parsing expiry:", err.Error())
				return
			}

			expiry = storage.Expiry{
				Time:    time.Now().Add(time.Duration(expiryMs) * time.Millisecond),
				Expires: true,
			}
		default:
			fmt.Println("Error sub-command: invalid command", subCmd)
			return
		}
	}

	db.Set(key, value, expiry)

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
