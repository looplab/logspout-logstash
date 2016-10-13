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

	tags := []string {}
	tags_str := os.Getenv("LOGSTASH_TAGS")

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_TAGS=") {
			tags_str = strings.TrimPrefix(e, "LOGSTASH_TAGS=")
			break
		}
	}

	if len(tags_str) > 0 {
		tags = strings.Split(tags_str, ",")
	}

	a.containerTags[c.ID] = tags
	return tags
}

// Get logstash fields configured with the environment variable LOGSTASH_FIELDS
func GetLogstashFields(c *docker.Container, a *LogstashAdapter) map[string]string {
	if fields, ok := a.logstashFields[c.ID]; ok {
		return fields
	}

	fields_str := os.Getenv("LOGSTASH_FIELDS")
	fields := map[string]string {}

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_FIELDS=") {
			fields_str = strings.TrimPrefix(e, "LOGSTASH_FIELDS=")
		}
	}

	if len(fields_str) > 0 {
		for _, f := range strings.Split(fields_str, ",") {
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

		// Parse JSON-encoded m.Data
		if err := json.Unmarshal([]byte(m.Data), &data); err != nil {
			
		    msg := make(map[string]interface{}) 

			msg["message"] = m.Data
			msg["docker"] = dockerInfo
		    msg["stream"] = m.Source
		    msg["tags"] = tags

			for k, v := range fields { 
			    msg[k] = v
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

			for k, v := range fields { 
			    data[k] = v
			}	
		    
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
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message string     `json:"message"`
	Stream  string     `json:"stream"`
	Docker  DockerInfo `json:"docker"`
	Tags    []string   `json:"tags"`
}
