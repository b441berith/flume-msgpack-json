# flume-msgpack-json

Flume interceptor to convert a stream of MessagePack events to JSON events.
Supports both uncompressed and GZIP streams + multiple msgpack records per event.

## Usage

Build a jar file

```
mvn package
```

Put the created flume-msgpack-json-1.0.0.jar file to Flume lib folder.
Also put the following jars:

- msgpack-core-0.8.3.jar
- jackson-core-2.7.1.jar
- jackson-dataformat-msgpack-0.8.3.jar
- jackson-databind-2.7.1.jar
- log4j-1.2.17.jar
- jackson-annotations-2.7.1.jar

Delete previous versions of jars mentioned above if needed

Add the following lines to your Flume configuration:

```
agent.sources.kafkaSource.interceptors = i1
agent.sources.kafkaSource.interceptors.i1.type = org.apache.flume.MessagePackToJSONInterceptor$Builder
agent.sources.kafkaSource.interceptors.i1.preserveExisting = false
```
