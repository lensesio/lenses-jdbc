package com.landoop.jdbc4

import java.util.*

interface ProducerSetup {

  fun props() = Properties().apply {
    this.put("bootstrap.servers", "localhost:9092")
    this.put("acks", "all")
    this.put("retries", 0)
    this.put("batch.size", 16384)
    this.put("linger.ms", 1)
    this.put("buffer.memory", 33554432)
    this.put("key.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer::class.java.canonicalName)
    this.put("value.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer::class.java.canonicalName)
    this.put("schema.registry.url", "http://127.0.0.1:8081")
  }

}