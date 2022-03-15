package client

import (
	"bufio"
	"encoding/json"
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

func (c *Client) ListStreams() ([]core.StreamInfo, error) {
	resp, err := http.Get(fmt.Sprintf("%s/streams", c.conf.Host))
	if err != nil {
		return nil, err
	}

	sInfos := make([]core.StreamInfo, 0)
	err = json.NewDecoder(resp.Body).Decode(&sInfos)
	return sInfos, err
}

type ConsumerConfig struct {
	Host  string
	Group string
}

type Consumer struct {
	conf             *ConsumerConfig
	currSubscription *http.Response
	sc               *bufio.Scanner
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

	resp, err := http.Get(uri)
	c.currSubscription = resp
	c.sc = bufio.NewScanner(resp.Body)
	return err
}

func (c *Consumer) Listen() (*core.Message, error) {
	if c.sc.Scan() {
		if err := c.sc.Err(); err != nil {
			return nil, err
		}

		jsonText := c.sc.Text()
		msg := &core.Message{}

		if err := json.Unmarshal([]byte(jsonText), msg); err != nil {
			return nil, err
		}
		return msg, nil
	}
	return nil, io.EOF
}

func (c *Consumer) Close() error {
	if c.currSubscription != nil {
		return c.currSubscription.Body.Close()
	}
	return nil
}
