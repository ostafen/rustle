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
	var err error
	r := api.CreateRouter()
	s := &http.Server{Addr: ":8080", Handler: r}

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

func createStream(sname string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/streams/%s", endpoint, sname), nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func deleteStream(sname string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/streams/%s", endpoint, sname), nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
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
		resp, err := createStream("stream:" + strconv.Itoa(i))
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, resp.StatusCode)
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
		resp, err := createStream(sname)
		require.NoError(t, err)
		require.Equal(t, http.StatusConflict, resp.StatusCode)

		resp, err = deleteStream(sname)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
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

	resp, err := createStream("test")
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	c := client.NewConsumer(&client.ConsumerConfig{
		Host: endpoint,
	})
	defer c.Close()

	n := 100
	go func() {
		time.Sleep(time.Millisecond * 100)
		for i := 0; i < n; i++ {
			resp, err = sendMessage("test", "ciao")
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

	resp, err := createStream("test")
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

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
			resp, err = sendMessage("test", "ciao")
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
