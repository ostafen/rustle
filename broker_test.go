package rustle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ostafen/rustle/api"
	"github.com/ostafen/rustle/client"
	"github.com/ostafen/rustle/core"
	"github.com/stretchr/testify/require"
)

func setupServer(t *testing.T) func() {
	broker := core.NewBroker()
	controller := api.NewController(broker)

	var err error
	s := &http.Server{Addr: ":8080", Handler: controller.GetRouter()}

	done := make(chan struct{}, 1)
	go func() {
		err = s.ListenAndServe()
		done <- struct{}{}
	}()
	time.Sleep(time.Millisecond * 10) // ensure that err has been set

	require.NoError(t, err)
	return func() {
		s.Shutdown(context.Background())
		<-done
	}
}

const endpoint = "http://localhost:8080"

func TestCreateStreamAndDelete(t *testing.T) {
	close := setupServer(t)
	defer close()

	cli := client.New(&client.ClientConfig{
		Host: endpoint,
	})

	n := 100
	for i := 0; i < n; i++ {
		require.NoError(t, cli.CreateStream("stream:"+strconv.Itoa(i)))
	}

	streamInfos, err := cli.ListStreams()
	require.NoError(t, err)

	infoMap := make(map[string]core.StreamInfo)
	for _, info := range streamInfos {
		infoMap[info.Name] = info
	}

	for i := 0; i < n; i++ {
		_, ok := infoMap["stream:"+strconv.Itoa(i)]
		require.True(t, ok)
	}

	for i := 0; i < n; i++ {
		sname := "stream:" + strconv.Itoa(i)
		require.Error(t, cli.CreateStream(sname))

		err := cli.DeleteStream(sname)
		require.NoError(t, err)
	}

	streamInfos, err = cli.ListStreams()
	require.NoError(t, err)

	require.Empty(t, streamInfos)
}

func sendMessage(sname string, body interface{}) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return http.Post(fmt.Sprintf("%s/streams/%s", endpoint, sname), "application/json", bytes.NewBuffer(data))
}

func TestStreamSubscription(t *testing.T) {
	close := setupServer(t)
	defer close()

	cli := client.New(&client.ClientConfig{
		Host: endpoint,
	})

	err := cli.CreateStream("test")
	require.NoError(t, err)

	c := client.NewConsumer(&client.ConsumerConfig{
		Host: endpoint,
	})
	defer c.Close()

	n := 100
	go func() {
		time.Sleep(time.Millisecond * 100)
		for i := 0; i < n; i++ {
			resp, err := sendMessage("test", "ciao")
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)
		}
	}()

	require.NoError(t, c.Subscribe("test"))

	count := 0
	for _, err := c.Listen(); err == nil; _, err = c.Listen() {
		count++
		if count >= n {
			break
		}
	}
	require.Equal(t, n, count)
	require.NoError(t, err)
}

func TestStreamSubscriptionWithGroup(t *testing.T) {
	close := setupServer(t)
	defer close()

	cli := client.New(&client.ClientConfig{
		Host: endpoint,
	})

	err := cli.CreateStream("test")
	require.NoError(t, err)

	const n = 100
	const nConsumers = n / 10
	const messagesPerConsumer = n / nConsumers

	consumers := make([]*client.Consumer, 0)
	for i := 0; i < nConsumers; i++ {
		c := client.NewConsumer(&client.ConsumerConfig{
			Host:  endpoint,
			Group: "test-group",
		})
		consumers = append(consumers, c)
	}

	defer func() {
		for _, c := range consumers {
			c.Close()
		}
	}()

	go func() {
		time.Sleep(time.Millisecond * 100)
		for i := 0; i < n; i++ {
			resp, err := sendMessage("test", "ciao")
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(nConsumers)

	var counters [nConsumers]int

	for i, c := range consumers {
		go func(i int, c *client.Consumer) {
			defer wg.Done()

			require.NoError(t, c.Subscribe("test"))

			counters[i] = 0
			for _, err := c.Listen(); err == nil; _, err = c.Listen() {
				counters[i]++
				if counters[i] >= messagesPerConsumer {
					break
				}
			}
		}(i, c)
	}

	wg.Wait()

	for i := 0; i < nConsumers; i++ {
		require.Equal(t, messagesPerConsumer, counters[i])
	}
}

func TestGetConsumerGroupInfo(t *testing.T) {
	close := setupServer(t)
	defer close()

	cli := client.New(&client.ClientConfig{
		Host: endpoint,
	})

	err := cli.CreateStream("test-stream")
	require.NoError(t, err)

	const nConsumers = 100

	consumers := make([]*client.Consumer, 0)
	for i := 0; i < nConsumers; i++ {
		c := client.NewConsumer(&client.ConsumerConfig{
			Host:  endpoint,
			Group: "test-group",
		})
		consumers = append(consumers, c)

		go func(c *client.Consumer) {
			c.Subscribe("test-stream")
		}(c)
	}

	time.Sleep(time.Millisecond * 100)

	info, err := cli.GetConsumerGroupInfo("test-group")
	require.NoError(t, err)
	require.Equal(t, nConsumers, len(info.Consumers))

	for _, c := range consumers {
		c.Close()
	}

	// wait a bit for server to clean up all consumer subscriptions
	time.Sleep(time.Millisecond * 10)

	info, err = cli.GetConsumerGroupInfo("test-group")
	require.NoError(t, err)
	require.Equal(t, 0, len(info.Consumers))
}
