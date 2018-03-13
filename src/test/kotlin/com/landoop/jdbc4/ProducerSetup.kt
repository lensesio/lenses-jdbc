package com.landoop.jdbc4

import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.*
import java.util.concurrent.TimeUnit
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient



interface ProducerSetup : Logging {

  fun schemaClient() = CachedSchemaRegistryClient("http://127.0.0.1:8081", 1000)

  fun newTopicName() = "topic_" + UUID.randomUUID().toString().replace('-', '_')

  fun createTopic(): String {
    val topic = newTopicName()
    val client = createAdmin()

    logger.debug("Creating topic $topic")
    val result = client.createTopics(listOf(NewTopic(topic, 1, 1)))

    logger.debug("Waiting on result")
    result.all().get(10, TimeUnit.SECONDS)

    logger.debug("Closing admin client")
    client.close(10, TimeUnit.SECONDS)

    return topic
  }

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