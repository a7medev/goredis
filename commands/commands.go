package commands

import (
	"fmt"
	"strconv"
	"strings"

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
		c.Reply(resp.NewSimpleError("ERR syntax error"))
		return
	}

	p.NextType()
	value, err := p.NextBulkString()

	if err != nil {
		fmt.Println("Error parsing value:", err.Error())
		c.Reply(resp.NewSimpleError("ERR syntax error"))
		return
	}

	expiry := storage.NeverExpires
	mode := storage.SetDefault
	get := false
	keepTTL := false

	// Parse extra arguments to SET
	for args > 2 {
		p.NextType()
		arg, err := p.NextBulkString()

		if err != nil {
			fmt.Println("Error parsing argument:", err.Error())
			c.Reply(resp.NewSimpleError("ERR syntax error"))
			return
		}

		arg = strings.ToUpper(arg)

		switch arg {
		case "NX":
			if mode == storage.SetXX {
				fmt.Println("Error parsing argument: Can't use both NX and XX")
				c.Reply(resp.NewSimpleError("ERR syntax error"))
				return
			}

			mode = storage.SetNX
		case "XX":
			if mode == storage.SetNX {
				fmt.Println("Error parsing argument: Can't use both NX and XX")
				c.Reply(resp.NewSimpleError("ERR syntax error"))
				return
			}

			mode = storage.SetXX

		case "GET":
			get = true

		case "EX", "PX", "EXAT", "PXAT":
			p.NextType()
			timeStr, err := p.NextBulkString()

			args--

			if err != nil {
				fmt.Println("Error parsing expiry:", err.Error())
				c.Reply(resp.NewSimpleError("ERR syntax error"))
				return
			}

			t, err := strconv.ParseInt(timeStr, 10, 64)

			if err != nil || t <= 0 {
				if err != nil {
					fmt.Println("Error parsing expiry:", err.Error())
				}
				c.Reply(resp.NewSimpleError("ERR invalid expire time in 'set' command"))
				return
			}

			expiry = storage.NewExpiry(t, arg)

		case "KEEPTTL":
			keepTTL = true
		}

		args--
	}

	result, exists, isSet := db.Set(key, value, expiry, mode, keepTTL, get)

	if get && exists {
		c.Reply(resp.NewBulkString(result))
	} else if get && !exists {
		c.Reply(resp.NewNullBulkString())
	} else if isSet {
		c.Reply(resp.NewSimpleString("OK"))
	} else {
		c.Reply(resp.NewNullBulkString())
	}
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

func Del(c *server.Connection, db *storage.Database, p *resp.Parser, args int) {
	deleted := 0

	for i := 0; i < args; i++ {
		p.NextType()
		key, err := p.NextBulkString()

		if err != nil {
			fmt.Println("Error parsing key: ", err.Error())
			c.Reply(resp.NewSimpleError("ERR syntax error"))
			return
		}

		if ok := db.Delete(key); ok {
			deleted++
		}
	}

	c.Reply(resp.NewInteger(deleted))
}
