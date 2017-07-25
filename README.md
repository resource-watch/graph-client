# Graph client Microservice


This repository is the node skeleton microservice to create node microservice for WRI API

1. [Getting Started](#getting-started)

## Getting Started

### OS X

**First, make sure that you have the [API gateway running
locally](https://github.com/control-tower/control-tower).**

We're using Docker which, luckily for you, means that getting the
application running locally should be fairly painless. First, make sure
that you have [Docker Compose](https://docs.docker.com/compose/install/)
installed on your machine.

```
git clone https://github.com/Vizzuality/graph-client
cd graph-client
./service.sh develop
./service.sh test
```text

You can now access the microservice through the CT gateway.

```

### Configuration

It is necessary to define these environment variables:

* CT_URL => Control Tower URL
* NODE_ENV => Environment (prod, staging, dev)

## Database constraints

Add the next constraints in Neo4j:

```
// only one dataset node with the same id
CREATE CONSTRAINT ON (dataset:Dataset) ASSERT dataset.id IS UNIQUE

// only one widget node with the same id
CREATE CONSTRAINT ON (widget:Widget) ASSERT widget.id IS UNIQUE

// only one layer node with the same id
CREATE CONSTRAINT ON (layer:Layer) ASSERT layer.id IS UNIQUE

// only one metadata node with the same id
CREATE CONSTRAINT ON (metadata:Metadata) ASSERT metadata.id IS UNIQUE


```
