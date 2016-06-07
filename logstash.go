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

func getopt(name, dfault string) string {
	value := os.Getenv(name)
	if value == "" {
		value = dfault
	}
	return value
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn  net.Conn
	route *router.Route
	tag   string
}

// NewLogstashAdapter creates a LogstashAdapter with UDP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	tag := getopt("LOGSTASH_TAG", "{{.ContainerName}}"+route.Options["append_tag"])
	
	if err != nil {
		return nil, err
	}

	return &LogstashAdapter{
		route: route,
		conn:  conn,
		tag:   tag,
	}, nil
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		dockerInfo := DockerInfo{
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
			Tag:      a.tag
		}
		var js []byte

		var jsonMsg map[string]interface{}
		err := json.Unmarshal([]byte(m.Data), &jsonMsg)
		if err != nil {
			// the message is not in JSON make a new JSON message
			msg := LogstashMessage{
				Message: m.Data,
				Docker:  dockerInfo,
				Stream:  m.Source,
			}
			js, err = json.Marshal(msg)
			if err != nil {
				log.Println("logstash:", err)
				continue
			}
		} else {
			// the message is already in JSON just add the docker specific fields as a nested structure
			jsonMsg["docker"] = dockerInfo

			js, err = json.Marshal(jsonMsg)
			if err != nil {
				log.Println("logstash:", err)
				continue
			}
		}
		_, err = a.conn.Write(js)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
	}
}

type DockerInfo struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Image    string `json:"image"`
	Hostname string `json:"hostname"`
	Tag      string `json:"tag"`
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message string     `json:"message"`
	Stream  string     `json:"stream"`
	Docker  DockerInfo `json:"docker"`
}
