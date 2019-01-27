package com.bell.cts.commons.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;

public class Consumer {

  private static final String APPLICATION_ID = "applicationId";

  public static void main(String[] args) {
    StreamsBuilder streamsBuilder = tableExample("test-topic");

    Properties config = createConfig();
    KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), config);
    kafkaStreams.setUncaughtExceptionHandler((t,
                                              e) -> System.out.println(String.format("Error: %s",
                                                                                     e.getStackTrace())));
    kafkaStreams.start();
    System.out.println("Streams is starting...");

    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    while (true) {
      kafkaStreams.localThreadsMetadata().forEach(System.out::println);
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  private static StreamsBuilder tableExample(String topic) {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.table(topic)
                  .toStream()
                  .foreach(((key, value) -> System.out.println(key + "###" + value)));
    return streamsBuilder;
  }

  private static StreamsBuilder joinTableExample(String topicLeft, String topicRight) {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    KTable<String, String> topicLeftTable = streamsBuilder.table(topicLeft);
    KTable<String, String> topicRightTable = streamsBuilder.table(topicRight);

    topicRightTable.join(topicLeftTable, (leftEvent, rightEvent) -> leftEvent + rightEvent)
                   .toStream()
                   .foreach(((key, value) -> System.out.println(key + "###" + value)));
    return streamsBuilder;
  }

  private static Properties createConfig() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10);
    return config;
  }

}
