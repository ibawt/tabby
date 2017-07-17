# tabby

[![Build Status](https://travis-ci.org/ibawt/tabby.svg?branch=master)](https://travis-ci.org/ibawt/tabby)
[![Coverage Status](https://coveralls.io/repos/github/ibawt/tabby/badge.svg)](https://coveralls.io/github/ibawt/tabby)

Tabby is an implementation of the Raft consesus algorithm.

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
