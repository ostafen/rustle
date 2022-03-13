package core

import (
	"io"
	"sync"
)

type Broker struct {
	mu      sync.Mutex
	streams map[string]*stream
	cGroups map[string]*cGroup
}

func NewBroker() *Broker {
	return &Broker{
		streams: make(map[string]*stream),
		cGroups: make(map[string]*cGroup),
	}
}

type StreamInfo struct {
	Name string `json:"name"`
	// TODO: add other infos
}

func (b *Broker) ListStreams() []StreamInfo {
	b.mu.Lock()
	defer b.mu.Unlock()

	streams := make([]StreamInfo, 0, len(b.streams))
	for name := range b.streams {
		streams = append(streams, StreamInfo{
			Name: name,
		})
	}
	return streams
}

func (b *Broker) HasStream(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, ok := b.streams[name]
	return ok
}

func (b *Broker) CreateStream(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.streams[name]; ok {
		return false
	}

	b.streams[name] = newStream()
	return true
}

func (b *Broker) getOrCreateGroup(name string) *cGroup {
	group, ok := b.cGroups[name]
	if !ok {
		group = newCGroup(name)
		b.cGroups[name] = group
	}
	return group
}

func (b *Broker) RegisterConsumer(cgroup string, w io.Writer, streams ...string) *consumer {
	b.mu.Lock()
	defer b.mu.Unlock()

	group := b.getOrCreateGroup(cgroup)
	c := group.newConsumer()

	for _, sname := range streams {
		stream, ok := b.streams[sname]
		if ok {
			stream.addGroup(group)
		}
	}
	c.start(w)
	return c
}

func (b *Broker) NotifyMessage(msg *Message) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// check has stream
	s, ok := b.streams[msg.Stream]
	if !ok {
		return
	}
	s.addMessage(msg)
}

func (b *Broker) CreateGroup(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.cGroups[name]; ok {
		return false
	}
	b.getOrCreateGroup(name)
	return true
}

func (b *Broker) DeleteGroup(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	group, ok := b.cGroups[name]
	if !ok {
		return
	}

	for _, stream := range b.streams {
		stream.deleteGroup(name)
	}
	delete(b.cGroups, name)

	b.mu.Unlock()

	group.shutdown()
}
