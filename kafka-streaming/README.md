# Spark Streaming with Kafka

This is a Spark Streaming job which streams weather data from Kafka
and stores into two Cassandra tables.

It borrows heavily from KillrWeather application found at https://github.com/killrweather/killrweather

#### To run on local machine

Download Kafka and then we're going to follow similar steps as found here
https://kafka.apache.org/quickstart

You'll need to update your path appropriately for the following commands
depending on where Kafka; i.e. where is Kafka `bin` dir

* Start Zookeeper ```bin/zookeeper-server-start.sh config/zookeeper.properties```
* Start Kafka ```bin/kafka-server-start.sh config/server.properties```
* Create Kafka topic ```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic raw_weather```
* Start Streaming job in SBT similar to described above.  Choose the `WeatherDataStream` option
* Send Weather Data to Kafka ```kafka-console-producer.sh --broker-list localhost:9092 --topic raw_weather
  --new-producer < ny-2008.csv```

#### Monitor with SparkLint
