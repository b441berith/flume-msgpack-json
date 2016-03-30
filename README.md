# flume-msgpack-json

Flume interceptor to convert a stream of MessagePack events to JSON events.

## Usage

Build a jar file

```
mvn package
```

Put the created flume-msgpack-json-<version>.jar file to Flume lib folder.
Also put the following jars:

- msgpack-core-0.8.3.jar
- jackson-dataformat-msgpack-0.8.3.jar
- jackson-databind-2.7.1.jar
- log4j-1.2.17.jar

Add the following lines to your Flume configuration:

```
agent.sources.kafkaSource.interceptors = i1
agent.sources.kafkaSource.interceptors.i1.type = org.apache.flume.MessagePackToJSONInterceptor
agent.sources.kafkaSource.interceptors.i1.preserveExisting = false
```