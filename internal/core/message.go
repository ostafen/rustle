package core

import (
	"time"

	uuid "github.com/satori/go.uuid"
)

type Message struct {
	Id        string
	Timestamp uint64      `json:"timestamp"`
	Stream    string      `json:"stream"`
	Data      interface{} `json:"data"`
}

func NewMessage(stream string, data interface{}) *Message {
	return &Message{
		Id:        uuid.NewV4().String(),
		Timestamp: uint64(time.Now().UnixNano()),
		Stream:    stream,
		Data:      data,
	}
}
