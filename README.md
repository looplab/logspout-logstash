[![GoDoc](https://godoc.org/github.com/looplab/logspout-logstash?status.svg)](https://godoc.org/github.com/looplab/logspout-logstash)
[![Go Report Card](https://goreportcard.com/badge/looplab/logspout-logstash)](https://goreportcard.com/report/looplab/logspout-logstash)

# logspout-logstash

A minimalistic adapter for github.com/gliderlabs/logspout to write to Logstash

Follow the instructions in https://github.com/gliderlabs/logspout/tree/master/custom on how to build your own Logspout container with custom modules. Basically just copy the contents of the custom folder and include:

```go
package main

import (
  _ "github.com/looplab/logspout-logstash"
  _ "github.com/gliderlabs/logspout/transports/udp"
  _ "github.com/gliderlabs/logspout/transports/tcp"
)
```

in modules.go.

Use by setting a docker environment variable `ROUTE_URIS=logstash://host:port` to the Logstash server.
The default protocol is UDP, but it is possible to change to TCP by adding ```+tcp``` after the logstash protocol when starting your container.

```bash
docker run --name="logspout" \
    --volume=/var/run/docker.sock:/var/run/docker.sock \
    -e ROUTE_URIS=logstash+tcp://logstash.home.local:5000 \
    localhost/logspout-logstash:v3.1
```

In your logstash config, set the input codec to `json` e.g:

```bash
input {
  udp {
    port  => 5000
    codec => json
  }
  tcp {
    port  => 5000
    codec => json
  }
}
```

## Available configuration options

For example, to get into the Logstash event's @tags field, use the ```LOGSTASH_TAGS``` container environment variable. Multiple tags can be passed by using comma-separated values

```bash
  # Add any number of arbitrary tags to your event
  -e LOGSTASH_TAGS="docker,production"
```

The output into logstash should be like:

```json
    "tags": [
      "docker",
      "production"
    ],
```

You can also add arbitrary logstash fields to the event using the ```LOGSTASH_FIELDS``` container environment variable:

```bash
  # Add any number of arbitrary fields to your event
  -e LOGSTASH_FIELDS="myfield=something,anotherfield=something_else"
```

The output into logstash should be like:

```json
    "myfield": "something",
    "another_field": "something_else",
```

Both configuration options can be set for every individual container, or for the logspout-logstash
container itself where they then become a default for all containers if not overridden there.

By setting the environment variable DOCKER_LABELS to a non-empty value, logspout-logstash will add all docker container
labels as fields:
```json
    "docker": {
        "hostname": "866e2ca94f5f",
        "id": "866e2ca94f5fe11d57add5a78232c53dfb6187f04f6e150ec15f0ae1e1737731",
        "image": "centos:7",
        "labels": {
            "a_label": "yes",
            "build-date": "20161214",
            "license": "GPLv2",
            "name": "CentOS Base Image",
            "pleasework": "okay",
            "some_label_with_dots": "more.dots",
            "vendor": "CentOS"
        },
        "name": "/ecstatic_murdock"
```

To be compatible with Elasticsearch, dots in labels will be replaced with underscores.

#### Using Logspout in a swarm

In a swarm, logspout is best deployed as a global service.  When running logspout with 'docker run', you can change the value of the hostname field using the `SYSLOG_HOSTNAME` environment variable as explained above. However, this does not work in a compose file because the value for `SYSLOG_HOSTNAME` will be the same for all logspout "tasks", regardless of the docker host on which they run. To support this mode of deployment, the syslog adapter will look for the file `/etc/host_hostname` and, if the file exists and it is not empty, will configure the hostname field with the content of this file. You can then use a volume mount to map a file on the docker hosts with the file `/etc/host_hostname` in the container.  The sample compose file below illustrates how this can be done

```
version: "3"
networks:
  logging:
services:
  logspout:
    image: localhost/logspout-logstash:latest
    networks:
      - logging
    volumes:
      - /etc/hostname:/etc/host_hostname:ro
      - /var/run/docker.sock:/var/run/docker.sock
    command:
      syslog://svt2-logger.am2.cloudra.local:514
    deploy:
      mode: global
      resources:
        limits:
          cpus: '0.20'
          memory: 256M
        reservations:
          cpus: '0.10'
          memory: 128M
```

logspout can then be deployed as a global service in the swarm with the following command

```bash
docker stack deploy --compose-file <name of your compose file> STACK
```

### Retrying

Two environment variables control the behaviour of Logspout when the Logstash target isn't available:
```RETRY_STARTUP``` causes Logspout to retry forever if Logstash isn't available at startup,
and ```RETRY_SEND``` will retry sending log lines when Logstash becomes unavailable while Logspout is running.
Note that ```RETRY_SEND``` will work only
if UDP is used for the log transport and the destination doesn't change;
in any other case ```RETRY_SEND``` should be disabled, restart and reconnect instead
and let ```RETRY_STARTUP``` deal with the situation.
With both retry options, log lines will be lost when Logstash isn't available. Set the
environment variables to any nonempty value to enable retrying. The default is disabled.


This table shows all available configurations:

| Environment Variable | Input Type | Default Value |
|----------------------|------------|---------------|
| LOGSTASH_TAGS        | array      | None          |
| LOGSTASH_FIELDS      | map        | None          |
| DOCKER_LABELS        | any        | ""            |
| RETRY_STARTUP        | any        | ""            |
| RETRY_SEND           | any        | ""            |
| DECODE_JSON_LOGS     | bool       | true          |
