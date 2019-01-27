package com.bell.cts.commons.kafka;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

public class Producer {

  private static final String TOPIC = "test-topic";

  public static void main(String[] args) throws InterruptedException {
    Properties config = new Properties();
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    String key = "key1";
    String value = "value1";
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(config);
    kafkaProducer.send(new ProducerRecord<>(TOPIC, key, value));

    TimeUnit.SECONDS.sleep(1);
  }

}
