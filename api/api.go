package api

import (
	"encoding/json"
	"github.com/ostafen/rustle/internal/core"
	"net/http"

	"github.com/gorilla/mux"
)

func writeJsonBody(rw http.ResponseWriter, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
	} else {
		rw.Write(data)
	}
}

type FlushWriter struct {
	w http.ResponseWriter
}

func (fw *FlushWriter) Write(data []byte) (int, error) {
	n, err := fw.w.Write(data)
	if err == nil {
		fw.w.(http.Flusher).Flush()
	}
	return n, err
}

type Controller struct {
	b *core.Broker
}

func handleStreamSubscription(c *Controller, rw http.ResponseWriter, r *http.Request) {
	// Set the headers related to event streaming.
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")

	fw := &FlushWriter{rw}

	cGroup := r.FormValue("cgroup")
	stream := mux.Vars(r)["name"]

	consumer, err := c.b.RegisterConsumer(cGroup, fw, stream)
	if err != nil {
		rw.WriteHeader(http.StatusNotFound)
		return
	}
	defer c.b.UnregisterConsumer(consumer)

	go func() {
		<-r.Context().Done()
		consumer.Stop()
	}()

	consumer.Join()
}

func handleStream(c *Controller, w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	switch r.Method {
	case "PUT":
		if c.b.CreateStream(name) {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusConflict)
		}
	case "POST":
		var body interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			c.b.NotifyMessage(core.NewMessage(name, body))
		}
	case "GET":
		/*if !c.b.HasStream(name) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			c.streamMessages(w, r, cgroup, name)
		}*/
	case "DELETE":
		c.b.DeleteStream(name)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func handleGroups(c *Controller, w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupName := vars["name"]

	switch r.Method {
	case "PUT":
		if c.b.CreateGroup(groupName) {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusConflict)
		}
	case "GET":
		info, err := c.b.GetConsumerGroupInfos(groupName)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
		} else {
			writeJsonBody(w, info)
		}
	case "DELETE":
		c.b.DeleteGroup(groupName)
	}
}

func handleListStreams(c *Controller, w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusBadRequest)
	}

	w.Header().Set("Content-Type", "application/json")

	channels := c.b.ListStreams()
	writeJsonBody(w, channels)
}

func handlePending(c *Controller, w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusBadRequest)
	}

	streamName := mux.Vars(r)["name"]
	groupName := r.FormValue("cgroup")

	pending, err := c.b.ListPending(streamName, groupName)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	} else {
		writeJsonBody(w, pending)
	}
}

func handleAck(c *Controller, w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
	}

	groupName := r.FormValue("cgroup")

	acks := make(map[string][]string)
	if err := json.NewDecoder(r.Body).Decode(&acks); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := c.b.AckMessages(groupName, acks); err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
}

func (c *Controller) handleListStreams() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handleListStreams(c, w, r)
	}
}

func (c *Controller) handleStreams() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handleStream(c, w, r)
	}
}

func (c *Controller) handleStreamSubscription() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handleStreamSubscription(c, w, r)
	}
}

func (c *Controller) handleGroups() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handleGroups(c, w, r)
	}
}

func (c *Controller) handlePending() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handlePending(c, w, r)
	}
}

func (c *Controller) handleAck() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handleAck(c, w, r)
	}
}

func NewController(broker *core.Broker) *Controller {
	return &Controller{
		b: broker,
	}
}

func (c *Controller) GetRouter() *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/streams", c.handleListStreams())
	r.HandleFunc("/streams/{name}", c.handleStreams())
	r.HandleFunc("/streams/{name}/messages", c.handleStreamSubscription())
	r.HandleFunc("/streams/{name}/messages/pending", c.handlePending())
	r.HandleFunc("/ack", c.handleAck())
	r.HandleFunc("/groups/{name}", c.handleGroups())
	return r
}
