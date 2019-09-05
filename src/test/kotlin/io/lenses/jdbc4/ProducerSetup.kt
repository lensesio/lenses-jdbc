package io.lenses.jdbc4

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.lenses.jdbc4.resultset.toList
import io.lenses.jdbc4.util.Logging
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import java.sql.Connection
import java.sql.DriverManager
import java.util.*
import java.util.concurrent.TimeUnit

interface ProducerSetup : Logging {

  fun conn(): Connection {
    LensesDriver()
    return DriverManager.getConnection("jdbc:lenses:kafka:http://localhost:24015", "admin", "admin")
  }

  fun schemaClient() = CachedSchemaRegistryClient("http://127.0.0.1:8081", 1000)

  fun registerValueSchema(topic: String, schema: Schema) {
    val client = schemaClient()
    val valueTopic = "$topic-value"
    client.register(valueTopic, schema)
    logger.debug("Schema registered at $valueTopic")
  }

  fun newTopicName() = "topic_" + UUID.randomUUID().toString().replace('-', '_')

  fun createTopic(topicName: String, compactMode: String = TopicConfig.CLEANUP_POLICY_COMPACT): String {
    createAdmin().use {

      logger.debug("Creating topic $topicName")
      it.createTopics(listOf(NewTopic(topicName, 1, 1))).all().get(10, TimeUnit.SECONDS)

      it.alterConfigs(mapOf(
          ConfigResource(ConfigResource.Type.TOPIC, topicName) to Config(
              listOf(ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, compactMode))
          )
      )).all().get()

      logger.debug("Closing admin client")
      it.close(10, TimeUnit.SECONDS)
    }

    conn().metaData.getTables(null, null, null, null).toList().map { it[2] }.contains(topicName)

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