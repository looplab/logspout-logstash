package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"

	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn  net.Conn
	route *router.Route
}

// NewLogstashAdapter creates a LogstashAdapter with UDP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &LogstashAdapter{
		route: route,
		conn:  conn,
	}, nil
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		var msg interface{}

		var jsonMsg map[string]interface{}
		err := json.Unmarshal([]byte(m.Data), &jsonMsg)
		if err != nil {
			// the message is not in JSON make a new JSON message
			msg = LogstashMessage{
				Message:  m.Data,
				Name:     m.Container.Name,
				ID:       m.Container.ID,
				Image:    m.Container.Config.Image,
				Hostname: m.Container.Config.Hostname,
			}

		} else {
			// the message is already in JSON just add the docker specific fields
			jsonMsg["docker.name"] = m.Container.Name
			jsonMsg["docker.id"] = m.Container.ID
			jsonMsg["docker.image"] = m.Container.Config.Image
			jsonMsg["docker.hostname"] = m.Container.Config.Hostname
			msg = jsonMsg
		}

		js, err := json.Marshal(msg)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
		_, err = a.conn.Write(js)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
	}
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message  string `json:"message"`
	Name     string `json:"docker.name"`
	ID       string `json:"docker.id"`
	Image    string `json:"docker.image"`
	Hostname string `json:"docker.hostname"`
}
