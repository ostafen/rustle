package rustle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/ostafen/rustle/api"
	"github.com/ostafen/rustle/core"
	"github.com/stretchr/testify/require"
)

func setupServer(t *testing.T) {
	var err error
	go func() {
		r := api.CreateRouter()
		err = http.ListenAndServe(":8080", r)
	}()
	require.NoError(t, err)
}

func createStream(sname string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/streams/%s", endpoint, sname), nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func listStreams() (map[string]core.StreamInfo, error) {
	resp, err := http.Get(fmt.Sprintf("%s/streams", endpoint))
	if err != nil {
		return nil, err
	}

	sInfos := make([]core.StreamInfo, 0)
	if err := json.NewDecoder(resp.Body).Decode(&sInfos); err != nil {
		return nil, err
	}

	infoMap := make(map[string]core.StreamInfo)
	for _, info := range sInfos {
		infoMap[info.Name] = info
	}
	return infoMap, err
}

const endpoint = "http://localhost:8080"

func TestTopicCreate(t *testing.T) {
	setupServer(t)

	n := 100
	for i := 0; i < n; i++ {
		resp, err := createStream("stream:" + strconv.Itoa(i))
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, resp.StatusCode)
	}

	streams, err := listStreams()
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		_, ok := streams["stream:"+strconv.Itoa(i)]
		require.True(t, ok)
	}

	for i := 0; i < n; i++ {
		resp, err := createStream("stream:" + strconv.Itoa(i))
		require.NoError(t, err)
		require.Equal(t, http.StatusConflict, resp.StatusCode)
	}
}
