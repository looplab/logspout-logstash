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

This table shows all available configurations:

| Environment Variable | Input Type | Default Value |
|----------------------|------------|---------------|
| LOGSTASH_TAGS        | array      | None          |
| LOGSTASH_FIELDS      | map        | None          |