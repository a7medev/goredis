package resp

import (
	"errors"
	"fmt"
	"strconv"
)

type DataType int32

const (
	SimpleString   DataType = 0
	SimpleError    DataType = 1
	Integer        DataType = 2
	BulkString     DataType = 3
	Array          DataType = 4
	Null           DataType = 5
	Boolean        DataType = 6
	Double         DataType = 7
	BigNumber      DataType = 8
	BulkError      DataType = 9
	VerbatimString DataType = 10
	Map            DataType = 11
	Set            DataType = 12
	Push           DataType = 13
)

type Parser struct {
	data []byte
	pos  int
	size int
}

func NewParser(data []byte, size int) *Parser {
	return &Parser{
		data: data,
		size: size,
		pos:  0,
	}
}

func (p *Parser) Reset() {
	p.pos = 0
}

func (p *Parser) SetPos(pos int) {
	p.pos = pos
}

func (p *Parser) NextType() (DataType, error) {
	t := p.data[p.pos]

	p.pos++

	switch t {
	case '+':
		return SimpleString, nil
	case '-':
		return SimpleError, nil
	case ':':
		return Integer, nil
	case '$':
		return BulkString, nil
	case '*':
		return Array, nil
	case '_':
		return Null, nil
	case '#':
		return Boolean, nil
	case ',':
		return Double, nil
	case '(':
		return BigNumber, nil
	case '!':
		return BulkError, nil
	case '=':
		return VerbatimString, nil
	case '%':
		return Map, nil
	case '~':
		return Set, nil
	case '>':
		return Push, nil
	default:
		return 0, errors.New("invalid redis data type")
	}
}

func (p *Parser) isCRLF(i int) bool {
	return p.data[i] == '\r' && p.data[i+1] == '\n'
}

func (p *Parser) NextSimpleString() (string, error) {
	for i := p.pos; i < p.size; i++ {
		if p.isCRLF(i) {
			// Skip the \r\n
			p.pos = i + 2
			return string(p.data[p.pos:i]), nil
		}
	}

	return "", errors.New("invalid simple string")
}

func (p *Parser) Debug() {
	fmt.Println("Data: ", strconv.Quote(string(p.data[:p.size])))
	fmt.Println("Pos: ", p.pos)
}

func (p *Parser) NextInteger() (int, error) {
	for i := p.pos; i < p.size; i++ {
		if p.isCRLF(i) {
			n, err := strconv.Atoi(string(p.data[p.pos:i]))

			if err != nil {
				return 0, err
			}

			p.pos = i + 2

			return n, nil
		}
	}

	return 0, errors.New("invalid integer")
}

func (p *Parser) NextBulkString() (string, error) {
	length, err := p.NextInteger()

	if err != nil {
		return "", err
	}

	result := string(p.data[p.pos : p.pos+length])

	// Skip the \r\n
	p.pos = p.pos + length + 2

	return result, nil
}
