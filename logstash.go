package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
	"strings"

	"github.com/fsouza/go-dockerclient"
	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn           net.Conn
	route          *router.Route
	containerTags  map[string][]string
	logstashFields map[string]map[string]string
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
		route:          route,
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
	}, nil
}

// Get container tags configured with the environment variable LOGSTASH_TAGS
func GetContainerTags(c *docker.Container, a *LogstashAdapter) []string {
	if tags, ok := a.containerTags[c.ID]; ok {
		return tags
	}

	tags := []string{}
	tagsStr := os.Getenv("LOGSTASH_TAGS")

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_TAGS=") {
			tagsStr = strings.TrimPrefix(e, "LOGSTASH_TAGS=")
			break
		}
	}

	if len(tagsStr) > 0 {
		tags = strings.Split(tagsStr, ",")
	}

	a.containerTags[c.ID] = tags
	return tags
}

// Get logstash fields configured with the environment variable LOGSTASH_FIELDS
func GetLogstashFields(c *docker.Container, a *LogstashAdapter) map[string]string {
	if fields, ok := a.logstashFields[c.ID]; ok {
		return fields
	}

	fieldsStr := os.Getenv("LOGSTASH_FIELDS")
	fields := map[string]string{}

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_FIELDS=") {
			fieldsStr = strings.TrimPrefix(e, "LOGSTASH_FIELDS=")
		}
	}

	if len(fieldsStr) > 0 {
		for _, f := range strings.Split(fieldsStr, ",") {
			sp := strings.Split(f, "=")
			k, v := sp[0], sp[1]
			fields[k] = v
		}
	}

	a.logstashFields[c.ID] = fields

	return fields
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {

	for m := range logstream {

		dockerInfo := DockerInfo{
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
		}

		tags := GetContainerTags(m.Container, a)
		fields := GetLogstashFields(m.Container, a)

		var js []byte
		var data map[string]interface{}
		var err error

		// Try to parse JSON-encoded m.Data. If it wasn't JSON, create an empty object
		// and use the original data as the message.
		if err = json.Unmarshal([]byte(m.Data), &data); err != nil {
			data = make(map[string]interface{})
			data["message"] = m.Data
		}

		for k, v := range fields {
			data[k] = v
		}

		data["docker"] = dockerInfo
		data["stream"] = m.Source
		data["tags"] = tags

		// Return the JSON encoding
		if js, err = json.Marshal(data); err != nil {
			// Log error message and continue parsing next line, if marshalling fails
			log.Println("logstash: could not marshal JSON:", err)
			continue
		}

		// To work with tls and tcp transports via json_lines codec
		js = append(js, byte('\n'))

		if _, err := a.conn.Write(js); err != nil {
			// There is no retry option implemented yet
			log.Fatal("logstash: could not write:", err)
		}
	}
}

type DockerInfo struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Image    string `json:"image"`
	Hostname string `json:"hostname"`
}
