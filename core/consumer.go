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

type consumerGroup struct {
	nextConsumerId uint64
	name           string
	consumers      map[uint64]*consumer
}

func newConsumerGroup(name string) *consumerGroup {
	return &consumerGroup{
		name:      name,
		consumers: make(map[uint64]*consumer),
	}
}

func (group *consumerGroup) addConsumer() *consumer {
	c := &consumer{
		group: group.name,
		id:    group.nextConsumerId,
		outCh: make(chan *Message, 1024),
		quit:  make(chan struct{}, 1),
	}
	group.consumers[group.nextConsumerId] = c
	group.nextConsumerId++
	return c
}

func (group *consumerGroup) removeConsumer(c *consumer) {
	delete(group.consumers, c.id)
}

func (group *consumerGroup) shutdown() {
	for _, c := range group.consumers {
		c.Stop()
	}
}
