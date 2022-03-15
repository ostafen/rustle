package core

import (
	"fmt"
	"io"
	"log"
	"sync"
)

type Broker struct {
	mu      sync.Mutex
	streams map[string]*stream
	cGroups map[string]*consumerGroup
}

func NewBroker() *Broker {
	return &Broker{
		streams: make(map[string]*stream),
		cGroups: make(map[string]*consumerGroup),
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

type ConsumerInfo struct {
	Id uint64 `json:"id"`
}

type ConsumerGroupInfo struct {
	Consumers []ConsumerInfo `json:"consumers"`
}

func (b *Broker) GetConsumerGroupInfos(name string) (*ConsumerGroupInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.cGroups[name]; !ok {
		return nil, fmt.Errorf("no consumer group with name \"%s\"", name)
	}

	cInfos := make([]ConsumerInfo, 0)
	group := b.cGroups[name]
	for _, c := range group.consumers {
		cInfos = append(cInfos, ConsumerInfo{Id: c.id})
	}

	return &ConsumerGroupInfo{
		Consumers: cInfos,
	}, nil
}

func (b *Broker) hasStream(name string) bool {
	_, ok := b.streams[name]
	return ok
}

func (b *Broker) HasStream(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.hasStream(name)
}

func (b *Broker) CreateStream(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.hasStream(name) {
		return false
	}

	b.streams[name] = newStream()
	return true
}

func (b *Broker) getOrCreateGroup(name string) *consumerGroup {
	group, ok := b.cGroups[name]
	if !ok {
		group = newConsumerGroup(name)
		b.cGroups[name] = group
	}
	return group
}

func (b *Broker) RegisterConsumer(cgroup string, w io.Writer, streams ...string) (*consumer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, stream := range streams {
		if !b.hasStream(stream) {
			return nil, fmt.Errorf("no such stream with name %s", stream)
		}
	}

	group := b.getOrCreateGroup(cgroup)
	c := group.addConsumerWithSubscriptions(streams)
	c.start(w)
	return c, nil
}

func (b *Broker) UnregisterConsumer(c *consumer) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.cGroups[c.group]; !ok {
		log.Fatal("trying to detach consumer on a non existing group")
	}

	b.cGroups[c.group].removeConsumer(c)
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

	for _, group := range b.cGroups {
		group.notify(msg)
	}
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

// TODO: do not allow to delete group if there are active consumers
func (b *Broker) DeleteGroup(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	group, ok := b.cGroups[name]
	if !ok {
		return
	}

	delete(b.cGroups, name)

	b.mu.Unlock()

	group.shutdown()
}

func (b *Broker) DeleteStream(sname string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.streams, sname)
}
