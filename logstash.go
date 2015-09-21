package logstash

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"github.com/benschw/dns-clb-go/clb"
	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	c 		clb
	conn	net.Conn
	route	*router.Route
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
		log.Fatal("Could not find udp transport for logstash module")
	}

	c := clb.New()

	address, err := c.GetAddress(route.Address)
	if err != nil {
	    panic(err)
	}

	conn, err := transport.Dial(address, route.Options)
	if err != nil {
		log.Fatal("Error dialing logstash address endpoint:", err)
	}

	return &LogstashAdapter{
		route:	route,
		conn:	conn,
		c:		c
	}, nil
}

func GetLogspoutOptionsString(env []string) string {
	if env != nil {
		for _, value := range env {
			if strings.HasPrefix(value, "LOGSPOUT_OPTIONS=") {
				return strings.TrimPrefix(value, "LOGSPOUT_OPTIONS=")
			}
		}
	}
	return ""
}

func UnmarshalOptions(opt_string string) map[string]string {
	var options map[string]string

	if opt_string != "" {
		b := []byte(opt_string)

		json.Unmarshal(b, &options)
		return options
	}
	return nil
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {

	options := UnmarshalOptions(getopt("OPTIONS", ""))

	resp, err := http.Get("http://169.254.169.254/latest/meta-data/instance-id")
	instance_id := ""
	if err == nil {
		value, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			instance_id = string(value)
		}
		resp.Body.Close()
	}

	for m := range logstream {
		container_options := UnmarshalOptions(GetLogspoutOptionsString(m.Container.Config.Env))

		// We give preference to the containers environment that is sending us the message
		if container_options == nil {
			container_options = options
		} else if options != nil {
			for k, v := range options {
				if _, ok := container_options[k]; !ok {
					container_options[k] = v
				}
			}
		}

		msg := LogstashMessage{
			Message:    m.Data,
			Name:       m.Container.Name,
			ID:         m.Container.ID,
			Image:      m.Container.Config.Image,
			Hostname:   m.Container.Config.Hostname,
			Args:       m.Container.Args,
			InstanceId: instance_id,
			Options:    container_options,
		}
		js, err := json.Marshal(msg)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}

		address, err := c.GetAddress(route.Address)
		if err != nil {
			log.Fatal("Could not resolve address for remote host", err)
		}

		if a.conn.RemoteAddr() != address {
			log.Println("Resolved address for remote host and connected address have changed. Updating to use new DNS resolution.")
			conn, err := transport.Dial(a.route.Address, a.route.Options)
		}

		_, err = a.conn.Write(js)
		if err != nil {
			transport, found := router.AdapterTransports.Lookup(a.route.AdapterTransport("udp"))
			if !found {
				log.Fatal("unable to find adapter: " + a.route.Adapter)
			}
			conn, err := transport.Dial(address, a.route.Options)
			if err != nil {
				log.Fatal("logstash:", err)
			}
			a.conn = conn
			_, err = a.conn.Write(js)
			if err != nil {
				log.Fatal("logstash - failure after reconnect:", err)
			}
		}
	}
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message    string            `json:"message"`
	Name       string            `json:"docker.name"`
	ID         string            `json:"docker.id"`
	Image      string            `json:"docker.image"`
	Hostname   string            `json:"docker.hostname"`
	Args       []string          `json:"docker.args,omitempty"`
	Options    map[string]string `json:"options,omitempty"`
	InstanceId string            `json:"instance-id,omitempty"`
}
