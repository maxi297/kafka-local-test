# kafka-local-testing

## Setup on MacOS
```
brew install kafka
brew services start zookeeper
brew services start kafka
```

## Command line
```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic --property print.key=true
kafka-console-producer --broker-list localhost:9092 --topic test-topic --property parse.key=true --property key.separator=:
```
