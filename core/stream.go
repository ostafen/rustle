package core

import "time"

type Message struct {
	Timestamp uint64      `json:"timestamp"`
	Stream    string      `json:"stream"`
	Data      interface{} `json:"data"`
}

func NewMessage(stream string, data interface{}) *Message {
	return &Message{
		Timestamp: uint64(time.Now().UnixNano()),
		Stream:    stream,
		Data:      data,
	}
}

type stream struct {
	msgs   []*Message
	groups map[string]*cGroup
}

func newStream() *stream {
	return &stream{
		msgs:   make([]*Message, 0, 100),
		groups: make(map[string]*cGroup),
	}
}

func (s *stream) addGroup(group *cGroup) {
	s.groups[group.name] = group
}

func (s *stream) deleteGroup(name string) {
	delete(s.groups, name)
}

func (s *stream) addMessage(msg *Message) {
	s.msgs = append(s.msgs, msg)

	for _, group := range s.groups {
		group.send(msg)
	}
}
