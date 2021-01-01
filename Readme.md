# Introduction
This connector uses spark structured streaming to listen to kafka topics which contains twitter stream data. The goal is to transform unstructured data into structured data that will be used to create connection graph.

#Configuration
create configuration file
`nano configuration.txt`

this sink connector is used to pull data from kafka topics in real-time and save it as parquet in HDFS
```
kafka.bootstrap.server=
kafka.source.topic=
trigger=
ouput.format=
keyword=

```

| Name | Description | Type | Importance |
|------|-------------|------|------------|
|kafka.bootstrap.server| Kafka server address| String| High |
|kafka.source.topic| Kafka topics which contain twitter stream data| String | High |


