package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"

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

func getopt(name, dfault string) string {
	value := os.Getenv(name)
	if value == "" {
		value = dfault
	}
	return value
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

	opt_string := getopt("OPTIONS", "")
	var options map[string]string

	if opt_string != "" {
		b := []byte(opt_string)

		if err := json.Unmarshal(b, &options); err != nil {
			options := nil
		}
	}

	for m := range logstream {
		msg := LogstashMessage{
			Message:  m.Data,
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
			Args:     m.Container.Args,
			Options:  options,
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
	Message  string            `json:"message"`
	Name     string            `json:"docker.name"`
	ID       string            `json:"docker.id"`
	Image    string            `json:"docker.image"`
	Hostname string            `json:"docker.hostname"`
	Args     []string          `json:"docker.args,omitempty"`
	Options  map[string]string `json:"options,omitempty"`
}
