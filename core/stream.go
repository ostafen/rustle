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

type streamConsumerGroup struct {
	consumers    []*consumer
	pending      map[uint64]struct{}
	nextConsumer int
}

func (l *streamConsumerGroup) next() int {
	next := l.nextConsumer % len(l.consumers)
	l.nextConsumer++
	return next
}

func (l *streamConsumerGroup) send(msg *Message) {
	l.pending[msg.Timestamp] = struct{}{}

	c := l.consumers[l.next()]
	c.send(msg)
}

type stream struct {
	msgs  []*Message
	group map[string]*streamConsumerGroup
}

func newStream() *stream {
	return &stream{
		msgs:  make([]*Message, 0, 100),
		group: make(map[string]*streamConsumerGroup),
	}
}

func (s *stream) addConsumer(cgroup string, c *consumer) {
	if s.group[cgroup] == nil {
		s.group[cgroup] = &streamConsumerGroup{
			consumers:    make([]*consumer, 0),
			pending:      map[uint64]struct{}{},
			nextConsumer: 0,
		}
	}

	l := s.group[cgroup]
	l.consumers = append(l.consumers, c)
}

func (s *stream) deleteGroup(name string) {
	delete(s.group, name)
}

func (s *stream) addMessage(msg *Message) {
	s.msgs = append(s.msgs, msg)

	for _, listeners := range s.group {
		listeners.send(msg)
	}
}
