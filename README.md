# tabby

[![Build Status](https://travis-ci.org/ibawt/tabby.svg?branch=master)](https://travis-ci.org/ibawt/tabby)
[![Coverage Status](https://coveralls.io/repos/github/ibawt/tabby/badge.svg)](https://coveralls.io/github/ibawt/tabby)
[![Clojars Project](https://img.shields.io/clojars/v/tabby.svg)](https://clojars.org/tabby)

Tabby is an implementation of the Raft consensus algorithm.

## Features
- REPL based development
- Leadership Election
- Log Replication

## Database
The database is just a key value store with support for the following operations:
- set key value
- get key
- compare-and-swap key new old

## Client Libraries
There is only one client at this time and it's written in clojure in this repo in the `client` namespace.

## Building
script/build

## Running
There are a few ways to run`tabby`
1. Kubernetes:
  There are some example yamls in the 'k8s' directory.
2. Command line:

For each peer machine run tabby like this:
`java -jar $TABBY_JAR --id $ME --peers $PEER_STRING --data-dir $DATA_DIR`

- $ME: The ID of peer in question
- $PEER_STRING: A comma separated list of peers in the format of <hostname>:<port>=<id>
                e.g. "tabby-0:7659=0,tabby-1:7659"
                NOTE: tabby will prune itself out of the peer list
- $DATA_DIR: Where tabby should store it's replicated log.

Ideally run this with a daemonizer, like systemd or whatever.

## TODO
- [ ] Fix inconsistent jepsen results
- [ ] HTTP client API
- [ ] HTTP health checks
- [ ] Prometheus Metrics

## Jespsen testing
1. Clone the jepsen repo
2. Copy the ./jepsen directory into the jepsen repo
3. Turn up jepsen, ( I use the docker one )
4. Enter the control node
5. Go into the 'tabby' subdirectory
6. `lein run test --concurrency 10`

## License

Copyright © 2015 Ian Quick

Distributed under the Eclipse Public License either version 1.0 or any later version.
