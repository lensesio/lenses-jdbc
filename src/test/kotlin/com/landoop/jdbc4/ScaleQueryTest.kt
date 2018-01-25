package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec
import org.apache.avro.LogicalTypes
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.sql.DriverManager
import java.util.*

class ScaleQueryTest : WordSpec(), QuerySetup {

  val topic = "topic_" + UUID.randomUUID().toString().replace('-', '_')

  fun populateEquities() {

    val amount = SchemaBuilder.builder().bytesType()
    LogicalTypes.decimal(4, 3).addToSchema(amount)

    val schema = SchemaBuilder.record("equity").fields()
        .name("a").type(amount).noDefault()
        .endRecord()

    val producer = KafkaProducer<String, GenericData.Record>(props())
    val record = GenericData.Record(schema)
    record.put("a", ByteBuffer.wrap(BigDecimal(12.34).unscaledValue().toByteArray()))
    producer.send(ProducerRecord<String, GenericData.Record>(topic, "key1", record))
  }

  init {

    LsqlDriver()
    populateEquities()

    val q = "SELECT * FROM $topic"
    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
    val stmt = conn.createStatement()

    "JDBC Driver" should {
      "support scale for logical decimals backed by bytes" {
        val rs = stmt.executeQuery(q)
        rs.next()
        rs.metaData.getScale(1) shouldBe 3
      }
    }
  }
}