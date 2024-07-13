package rdb

import "fmt"

type RDB struct {
	content []byte
}

func NewRDB(content []byte) *RDB {
	return &RDB{content: content}
}

func (r *RDB) Encode() string {
	return fmt.Sprintf("$%v\r\n%v", len(r.content), string(r.content))
}
