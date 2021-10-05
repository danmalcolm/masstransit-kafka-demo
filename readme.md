# MassTransit Kafka Demo

## Introduction

Demonstrates use of MassTransit Kafka rider to produce and consume messages, including support for multiple message types on a single topic.

Support for multiple event types per topic implemented via a custom Avro serializer. Note that a similar approach is needed when using the Confluent.Kafka libraries directly. See https://github.com/danmalcolm/confluent-kafka-dotnet/tree/multiple-message-types-on-topic.

## Implementation Notes


## Prerequisites

Development environment requires Docker support, e.g. Docker Desktop on windows and docker compose.

Execute the following from command prompt within solution directory to start Kafka infrastructure (use `docker-compose` if your version of docker does not support the compose command):

`docker compose up -d`

It should then be possible to run the MassTransitKafkaDemo project.

## Generating Avro classes

Install the tool apache.avro.tools 1.10 or higher (Apache tool supports logical types in avsc files)

This can be installed with:

```
dotnet tool install --global Apache.Avro.Tools --version 1.10.2
```

Generate types:

`avrogen -s TaskRequested.V1.avsc ..\;avrogen -s TaskStarted.V1.avsc ..\;avrogen -s TaskCompleted.V1.avsc ..\;`

## Browsing schema registry

Examples using Powershell.

All subjects:

`Invoke-RestMethod -Uri http://localhost:8081/subjects`

Latest version:

`Invoke-RestMethod -Uri http://localhost:8081/subjects/MassTransitKafkaDemo.Messages.TaskRequested/versions/latest | Format-List`



