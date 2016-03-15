package logstash

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn     net.Conn
	route    *router.Route
	hostname string
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
	var hostname string
	content, err := ioutil.ReadFile("/etc/hostname")
	if err != nil {
		log.Println("Could not read hostname, using default 'unknown'")
		hostname = "unknown"
	} else {
		hostname = string(content)
	}

	return &LogstashAdapter{
		route:    route,
		conn:     conn,
		hostname: hostname,
	}, nil
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		dockerInfo := DockerInfo{
			Name:     strings.Trim(m.Container.Name, "/"),
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
		}
		//var js []byte

		//var jsonMsg map[string]interface{}
		//err := json.Unmarshal([]byte(m.Data), &jsonMsg)
		//if err != nil {
		// the message is not in JSON make a new JSON message
		msg := LogstashMessage{
			Message:   strconv.Quote(m.Data),
			Docker:    dockerInfo,
			Source:    m.Source,
			Node:      a.hostname,
			Timestamp: m.Time,
		}
		js, err := json.Marshal(msg)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
		// } else {
		// 	// the message is already in JSON just add the docker specific fields as a nested structure
		// 	jsonMsg["docker"] = dockerInfo

		// 	js, err = json.Marshal(jsonMsg)
		// 	if err != nil {
		// 		log.Println("logstash:", err)
		// 		continue
		// 	}
		// }
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
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message   string     `json:"message"`
	Docker    DockerInfo `json:"docker"`
	Source    string     `json:"source"`
	Node      string     `json:"node"`
	Timestamp time.Time  `json:"@timestamp"`
}
