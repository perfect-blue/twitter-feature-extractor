# Introduction
This connector uses spark structured streaming to listen to kafka topics which contains twitter stream data. The goal is to transform unstructured data into structured data that will be used to create connection graph.

# Configuration
create configuration file
```
nano configuration.txt
```

this sink connector is used to pull data from kafka topics in real-time and save it as files in HDFS
```
kafka.bootstrap.server=
kafka.source.topic=
trigger=
ouput.format=
keyword=

```

| Name | Description | Type |default| Importance |
|------|-------------|------|-------|------------|
|kafka.bootstrap.server| Kafka server address| String|null| High |
|kafka.source.topic| Kafka topics which contain twitter stream data| String |null| High |
|trigger |Save Interval, how many times a file will be written to HDFS | Int |0| High|
|output.format |File format CSV or Parquet| String | null | High | 
|keyword | save file into seperate 'keyword' folder| null | Low |

# Output Schemas
## Edge
return the table of graph's edge

| Column Name | Optional | Type|Documentation|
|-------------|----------|-----|--------------|
|Source | False | String | User who initiate interaction|
|Target | False | String | User to whom interaction being initiated|
|Interaction| String | False |Type of interaction between 2 users (Retweet, Quote, Reply)|

## Nodes
Return the table of graph's node
|Column Name | Optional | Type | Documentation |
|------------|----------|------|---------------|
| CreatedAt | False |Date Time|Tweet creation time|
| TweetId | False | String| Id of a tweet |
| Text | False | String | Tweet status |
| ScreenName | False | String | Username |
| FollowersCount | False | Int | number of followers|
| FriendsCount  | False | Int | number of followings |
| Verified | False | Boolean | is user verified |
| RetweeetObject | True | [String] | Retweeted User information |
| QuoteObject | True | [String] | Quoted user information |

# Running
```
spark-submit --class Main \
--master <master-url> \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.5 \
Twitter-Feature-Extractor-v1.0 configuration.txt
```





