package core

import (
	"encoding/json"
	"io"
	"sync"
)

type consumer struct {
	group string
	id    uint64
	outCh chan *Message
	quit  chan struct{}
	wg    sync.WaitGroup
}

func (c *consumer) start(w io.Writer) {
	c.wg.Add(1)

	go func() {
		defer func() {
			c.wg.Done()
		}()

		for {
			select {
			case <-c.quit:
				return
			case msg := <-c.outCh:
				data, err := json.Marshal(msg)
				if err != nil {
					return
				}

				_, err = w.Write([]byte(string(data) + "\n"))
				if err != nil {
					return
				}
			}
		}
	}()
}

func (c *consumer) send(msg *Message) {
	c.outCh <- msg
}

func (c *consumer) Join() {
	c.wg.Wait()
}

func (c *consumer) Stop() {
	select {
	case c.quit <- struct{}{}:
	default:
	}
}

type streamSubscription struct {
	consumers    []*consumer
	pending      map[uint64]struct{}
	nextConsumer int
}

func (s *streamSubscription) add(c *consumer) {
	s.consumers = append(s.consumers, c)
}

func (s *streamSubscription) remove(id uint64) {
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

func (l *streamSubscription) next() int {
	next := l.nextConsumer % len(l.consumers)
	l.nextConsumer++
	return next
}

func (l *streamSubscription) send(msg *Message) {
	l.pending[msg.Timestamp] = struct{}{}

	c := l.consumers[l.next()]
	c.send(msg)
}

type consumerGroup struct {
	nextConsumerId uint64
	name           string
	consumers      map[uint64]*consumer
	subscriptions  map[string]*streamSubscription
}

func newConsumerGroup(name string) *consumerGroup {
	return &consumerGroup{
		name:          name,
		consumers:     make(map[uint64]*consumer),
		subscriptions: make(map[string]*streamSubscription),
	}
}

func (group *consumerGroup) getOrCreateSubscription(sname string) *streamSubscription {
	if _, ok := group.subscriptions[sname]; !ok {
		group.subscriptions[sname] = &streamSubscription{
			nextConsumer: 0,
			consumers:    make([]*consumer, 0),
			pending:      make(map[uint64]struct{}),
		}
	}
	return group.subscriptions[sname]
}

func (group *consumerGroup) addConsumerWithSubscriptions(streams []string) *consumer {
	c := &consumer{
		group: group.name,
		id:    group.nextConsumerId,
		outCh: make(chan *Message, 1024),
		quit:  make(chan struct{}, 1),
	}
	group.consumers[group.nextConsumerId] = c
	group.nextConsumerId++

	for _, stream := range streams {
		s := group.getOrCreateSubscription(stream)
		s.add(c)
	}
	return c
}

func (group *consumerGroup) removeConsumer(c *consumer) {
	delete(group.consumers, c.id)

	for sname, subscription := range group.subscriptions {
		subscription.remove(c.id)

		if len(subscription.consumers) == 0 {
			delete(group.subscriptions, sname)
		}
	}
}

func (group *consumerGroup) notify(msg *Message) {
	if subscription, ok := group.subscriptions[msg.Stream]; ok {
		subscription.send(msg)
	}
}

func (group *consumerGroup) shutdown() {
	for _, c := range group.consumers {
		c.Stop()
	}
}
