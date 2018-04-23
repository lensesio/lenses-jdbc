package com.landoop.jdbc4

import com.landoop.jdbc4.client.RestClient
import com.landoop.jdbc4.client.domain.Credentials
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.*
import java.util.concurrent.TimeUnit
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import java.io.Closeable


interface ProducerSetup : Logging {

  fun schemaClient() = CachedSchemaRegistryClient("http://127.0.0.1:8081", 1000)
  fun restClient() = RestClient(listOf("http://localhost:3030"), Credentials("admin", "admin"), true)

  fun registerValueSchema(topic: String, schema: Schema) {
    val client = schemaClient()
    val valueTopic = "$topic-value"
    client.register(valueTopic, schema)
    logger.debug("Schema registered at $valueTopic")
  }

  fun newTopicName() = "topic_" + UUID.randomUUID().toString().replace('-', '_')

  fun createTopic(topicName: String): String {
    createAdmin().use { it:AdminClient ->
      logger.debug("Creating topic $topicName")
      val result = it.createTopics(listOf(NewTopic(topicName, 1, 1)))

      logger.debug("Waiting on result")
      result.all().get(10, TimeUnit.SECONDS)

      logger.debug("Closing admin client")
      it.close(10, TimeUnit.SECONDS)
    }

    fun topicInLenses(): Boolean = restClient().use { it :RestClient ->
      try {
        it.topic(topicName)
        true
      } catch (e: Exception) {
        false
      }
    }

    while (!topicInLenses()) {
      Thread.sleep(3000)
    }

    return topicName
  }

  fun createTopic(): String = createTopic(newTopicName())

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