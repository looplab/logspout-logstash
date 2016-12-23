package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"strings"
	"os"

	"github.com/fsouza/go-dockerclient"
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

func filterTags(tags []string) []string {
	out := []string{}
	for _, t := range tags {
		if (strings.TrimSpace(t) != "") {
			out = append(out, t)
		}
	}
	return out
}

func multivalOpt(name string) []string {
	return filterTags(strings.Split(getopt(name, ""), ","))
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn          net.Conn
	route         *router.Route
	containerTags map[string][]string
	globalTags    []string
	includeLabels []string
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
		route:         route,
		conn:          conn,
		containerTags: make(map[string][]string),
		globalTags:    multivalOpt("LOGSTASH_GLOBAL_TAGS"),
		includeLabels: multivalOpt("LOGSTASH_INCLUDE_LABELS"),
	}, nil
}

// Get container tags configured with the environment variable LOGSTASH_TAGS
// ... and global tags configured on the logspout container.
func GetContainerTags(c *docker.Container, a *LogstashAdapter) []string {
	if tags, ok := a.containerTags[c.ID]; ok {
		return tags
	}

	var tags = []string{}
	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_TAGS=") {
			tags = strings.Split(strings.TrimPrefix(e, "LOGSTASH_TAGS="), ",")
			break
		}
	}

	tags = append(tags, a.globalTags...)

	a.containerTags[c.ID] = tags
	return tags
}

// Get container labels as configured with LOGSTASH_INCLUDE_LABELS
func GetContainerLabels(c *docker.Container, a *LogstashAdapter) map[string]string {
	var labels map[string]string
	labels = make(map[string]string)

	for _, label := range a.includeLabels {
		_, exists := c.Config.Labels[label]
		if exists {
			labels[label] = c.Config.Labels[label]
		}
	}

	return labels
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {

	for m := range logstream {

		dockerInfo := DockerInfo{
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
			Labels:   GetContainerLabels(m.Container, a),
		}

		tags := GetContainerTags(m.Container, a)

		var js []byte
		var data map[string]interface{}

		// Parse JSON-encoded m.Data
		if err := json.Unmarshal([]byte(m.Data), &data); err != nil {
			// The message is not in JSON, make a new JSON message.
			msg := LogstashMessage{
				Message: m.Data,
				Docker:  dockerInfo,
				Stream:  m.Source,
				Tags:    tags,
			}

			if js, err = json.Marshal(msg); err != nil {
				// Log error message and continue parsing next line, if marshalling fails
				log.Println("logstash: could not marshal JSON:", err)
				continue
			}
		} else {
			// The message is already in JSON, add the docker specific fields.
			data["docker"] = dockerInfo
			data["tags"] = tags
			data["stream"] = m.Source
			// Return the JSON encoding
			if js, err = json.Marshal(data); err != nil {
				// Log error message and continue parsing next line, if marshalling fails
				log.Println("logstash: could not marshal JSON:", err)
				continue
			}
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
	Labels   map[string]string `json:"labels"`
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message string     `json:"message"`
	Stream  string     `json:"stream"`
	Docker  DockerInfo `json:"docker"`
	Tags    []string   `json:"tags"`
}
