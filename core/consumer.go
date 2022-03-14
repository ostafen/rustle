package core

import (
	"encoding/json"
	"io"
	"sync"
)

type consumer struct {
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

type cGroup struct {
	name         string
	pending      map[uint64]struct{}
	nextConsumer int
	consumers    []*consumer
}

func newCGroup(name string) *cGroup {
	return &cGroup{
		name:         name,
		pending:      map[uint64]struct{}{},
		consumers:    make([]*consumer, 0),
		nextConsumer: 0,
	}
}

func (group *cGroup) newConsumer() *consumer {
	c := &consumer{
		outCh: make(chan *Message, 1024),
		quit:  make(chan struct{}, 1),
	}
	group.consumers = append(group.consumers, c)
	return c
}

func (group *cGroup) next() int {
	next := group.nextConsumer % len(group.consumers)
	group.nextConsumer++
	return next
}

func (group *cGroup) send(msg *Message) {
	group.pending[msg.Timestamp] = struct{}{}

	c := group.consumers[group.next()]
	c.send(msg)
}

func (group *cGroup) shutdown() {
	for _, c := range group.consumers {
		c.Stop()
	}
}
