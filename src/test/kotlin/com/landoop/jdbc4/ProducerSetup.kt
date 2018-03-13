package com.landoop.jdbc4

import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.*

interface ProducerSetup {

  fun adminProps() = Properties().apply {
    this["bootstrap.servers"] = "PLAINTEXT://127.0.0.1:9092"
  }

  fun createAdmin(): AdminClient = AdminClient.create(adminProps())

  fun producerProps() = Properties().apply {
    this["bootstrap.servers"] = "PLAINTEXT://127.0.0.1:9092"
    this["acks"] = "all"
    this["retries"] = 0
    this["batch.size"] = 16384
    this["linger.ms"] = 1
    this["buffer.memory"] = 33554432
    this["key.serializer"] = io.confluent.kafka.serializers.KafkaAvroSerializer::class.java.canonicalName
    this["value.serializer"] = io.confluent.kafka.serializers.KafkaAvroSerializer::class.java.canonicalName
    this["schema.registry.url"] = "http://127.0.0.1:8081"
  }

  fun createProducer() = KafkaProducer<String, GenericData.Record>(producerProps())
}