package core

import (
	"log"
)

type streamConsumerGroup struct {
	consumers    []*consumer
	pending      map[uint64]struct{}
	nextConsumer int
}

func (s *streamConsumerGroup) add(c *consumer) {
	s.consumers = append(s.consumers, c)
}

func (s *streamConsumerGroup) remove(id uint64) {
	for i, c := range s.consumers {
		if c.id == id {
			temp := s.consumers[0]
			s.consumers[0] = s.consumers[i]
			s.consumers[i] = temp
			s.consumers = s.consumers[1:]
			return
		}
	}
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

func (s *stream) attachConsumer(cgroup string, c *consumer) {
	if s.group[cgroup] == nil {
		s.group[cgroup] = &streamConsumerGroup{
			consumers:    make([]*consumer, 0),
			pending:      map[uint64]struct{}{},
			nextConsumer: 0,
		}
	}
	s.group[cgroup].add(c)
}

func (s *stream) detachConsumer(c *consumer) {
	if s.group[c.group] == nil {
		log.Fatal("detach consumer")
	}
	s.group[c.group].remove(c.id)
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
