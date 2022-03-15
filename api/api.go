package api

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/ostafen/rustle/core"
)

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
	name := vars["name"]

	switch r.Method {
	case "PUT":
		if c.b.CreateGroup(name) {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusConflict)
		}
	case "DELETE":
		c.b.DeleteGroup(name)
	}
}

func handleListStreams(c *Controller, w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusBadRequest)
	}

	w.Header().Set("Content-Type", "application/json")

	channels := c.b.ListStreams()
	text, err := json.Marshal(channels)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.Write(text)
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

func CreateRouter() *mux.Router {
	c := &Controller{b: core.NewBroker()}

	r := mux.NewRouter()
	r.HandleFunc("/streams", c.handleListStreams())
	r.HandleFunc("/streams/{name}", c.handleStreams())
	r.HandleFunc("/streams/{name}/messages", c.handleStreamSubscription())
	r.HandleFunc("/groups/{name}", c.handleGroups())

	return r
}
