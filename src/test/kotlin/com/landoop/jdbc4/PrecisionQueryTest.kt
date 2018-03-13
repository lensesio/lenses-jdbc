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

data class Equity(val ticker: String, val amount: BigDecimal)

class PrecisionQueryTest : WordSpec(), ProducerSetup {

  val topic = "topic_" + UUID.randomUUID().toString().replace('-', '_')

  fun populateEquities() {

    val equities = listOf(Equity("goog", BigDecimal(99.11)))

    val foo = SchemaBuilder.fixed("foo").size(5)
    val amount = SchemaBuilder.builder().bytesType()
    LogicalTypes.decimal(10, 4).addToSchema(amount)

    val schema = SchemaBuilder.record("equity").fields()
        .requiredString("ticker")
        .name("amount").type(amount).noDefault()
        .name("foo").type(foo).noDefault()
        .requiredBoolean("boolean")
        .requiredBytes("bytes")
        .requiredDouble("double")
        .requiredInt("int")
        .requiredLong("long")
        .endRecord()

    val producer = KafkaProducer<String, GenericData.Record>(producerProps())
    for (equity in equities) {

      val fixed = GenericData.Fixed(foo)
      fixed.bytes(byteArrayOf(1, 2, 3, 4, 5))

      val record = GenericData.Record(schema)
      record.put("ticker", equity.ticker)
      record.put("amount", ByteBuffer.wrap(equity.amount.unscaledValue().toByteArray()))
      record.put("foo", fixed)
      record.put("boolean", true)
      record.put("bytes", ByteBuffer.wrap(byteArrayOf(1, 2, 3)))
      record.put("double", 4.6523)
      record.put("int", 123)
      record.put("long", 556L)
      producer.send(ProducerRecord<String, GenericData.Record>(topic, equity.ticker, record))
    }
  }

  init {

    LsqlDriver()
    populateEquities()

    val q = "SELECT * FROM $topic"
    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
    val stmt = conn.createStatement()

    "JDBC Driver" should {
      "support precision for strings" {
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 8
        rs.metaData.getColumnLabel(1) shouldBe "ticker"
        rs.metaData.getColumnLabel(2) shouldBe "amount"
        rs.metaData.getColumnLabel(3) shouldBe "foo"
        rs.metaData.getColumnLabel(4) shouldBe "boolean"
        rs.metaData.getColumnLabel(5) shouldBe "bytes"
        rs.metaData.getColumnLabel(6) shouldBe "double"
        rs.metaData.getColumnLabel(7) shouldBe "int"
        rs.metaData.getColumnLabel(8) shouldBe "long"
        rs.next()
        // our strings are unlimited
        rs.metaData.getPrecision(1) shouldBe Int.MAX_VALUE
      }
      "support precision for logical decimals backed by bytes" {
        val rs = stmt.executeQuery(q)
        rs.next()
        rs.metaData.getPrecision(2) shouldBe 10
      }
      "support precision for fixed types" {
        val rs = stmt.executeQuery(q)
        rs.next()
        rs.metaData.getPrecision(3) shouldBe 5
      }
      "return 0 for all other types" {
        val rs = stmt.executeQuery(q)
        rs.next()
        rs.metaData.getPrecision(4) shouldBe 0
        rs.metaData.getPrecision(5) shouldBe 0
        rs.metaData.getPrecision(6) shouldBe 0
        rs.metaData.getPrecision(7) shouldBe 0
        rs.metaData.getPrecision(8) shouldBe 0
      }
    }
  }
}