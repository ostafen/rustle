package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/ostafen/rustle/core"
)

type ClientConfig struct {
	Host string
}

type Client struct {
	conf *ClientConfig
}

func New(conf *ClientConfig) *Client {
	return &Client{
		conf: conf,
	}
}

func (c *Client) CreateStream(sname string) error {
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/streams/%s", c.conf.Host, sname), nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("an error occured during creation of stream %s", sname)
	}
	return err
}

func (c *Client) DeleteStream(sname string) error {
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/streams/%s", c.conf.Host, sname), nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("an error occured during deletion of stream %s", sname)
	}
	return err
}

func (c *Client) ListStreams() ([]core.StreamInfo, error) {
	resp, err := http.Get(fmt.Sprintf("%s/streams", c.conf.Host))
	if err != nil {
		return nil, err
	}

	sInfos := make([]core.StreamInfo, 0)
	err = json.NewDecoder(resp.Body).Decode(&sInfos)
	return sInfos, err
}

func (c *Client) ListPendingQueue(sname string, group string) ([]string, error) {
	resp, err := http.Get(fmt.Sprintf("%s/streams/%s/messages/pending?cgroup=%s", c.conf.Host, sname, group))
	if err != nil {
		return nil, err
	}
	pending := make([]string, 0)
	err = json.NewDecoder(resp.Body).Decode(&pending)
	return pending, err
}

func (c *Client) GetConsumerGroupInfo(cgroup string) (*core.ConsumerGroupInfo, error) {
	resp, err := http.Get(fmt.Sprintf("%s/groups/%s", c.conf.Host, cgroup))
	if err != nil {
		return nil, err
	}
	info := &core.ConsumerGroupInfo{}
	err = json.NewDecoder(resp.Body).Decode(info)
	return info, err
}

type ConsumerConfig struct {
	Host  string
	Group string
}

type subscription struct {
	resp   *http.Response
	ctx    context.Context
	cancel context.CancelFunc
	sc     *bufio.Scanner
}

type Consumer struct {
	conf *ConsumerConfig
	s    *subscription
}

func NewConsumer(c *ConsumerConfig) *Consumer {
	return &Consumer{
		conf: c,
	}
}

func (c *Consumer) Subscribe(stream string) error {
	uri := fmt.Sprintf("%s/streams/%s/messages", c.conf.Host, stream)
	if c.conf.Group != "" {
		uri += "?cgroup=" + c.conf.Group
	}

	client := &http.Client{}
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.s = &subscription{
		ctx:    ctx,
		cancel: cancel,
	}
	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if resp != nil {
		c.s.resp = resp
		c.s.sc = bufio.NewScanner(resp.Body)
	}
	return err
}

var ErrNoActiveSubscription = errors.New("no active subscription")

func (c *Consumer) Listen() (*core.Message, error) {
	if c.s == nil {
		return nil, ErrNoActiveSubscription
	}

	s := c.s
	if s.sc.Scan() {
		if err := s.sc.Err(); err != nil {
			return nil, err
		}

		jsonText := s.sc.Text()
		msg := &core.Message{}

		if err := json.Unmarshal([]byte(jsonText), msg); err != nil {
			return nil, err
		}
		return msg, nil
	}
	return nil, io.EOF
}

func (c *Client) Ack(cgroup string, ackMap map[string][]string) error {
	data, err := json.Marshal(ackMap)
	if err != nil {
		return err
	}

	resp, err := http.Post(fmt.Sprintf("%s/ack?cgroup=%s", c.conf.Host, cgroup), "application/json", bytes.NewBuffer(data))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unable to ack messages")
	}
	return err
}

func (c *Consumer) Close() error {
	s := c.s
	if s == nil {
		return nil
	}

	if s.cancel != nil { // this is executed even if request succeeds
		s.cancel()
		<-s.ctx.Done()
		s.cancel = nil
	}

	if s.resp != nil {
		err := s.resp.Body.Close()
		s.resp.Body = nil
		return err
	}
	return nil
}
