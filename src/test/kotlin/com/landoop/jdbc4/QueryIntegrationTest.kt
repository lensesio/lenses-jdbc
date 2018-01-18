package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.sql.DriverManager
import java.util.*

data class Starship(val name: String, val designation: String)

class QueryIntegrationTest : WordSpec() {

  init {

    // populate kafka with some data so we can test it
    val props = Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", 0)
    props.put("batch.size", 16384)
    props.put("linger.ms", 1)
    props.put("buffer.memory", 33554432)
    props.put("key.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer::class.java.simpleName)
    props.put("value.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer::class.java.simpleName)
    props.put("schema.registry.url", "http://127.0.0.1:8081")

    val starships = listOf(
        Starship("USS Enterprise", "1701D"),
        Starship("USS Discovery", "1031")
    )

    val producer = KafkaProducer<String, String>(props)
    for (starship in starships) {
      producer.send(ProducerRecord<String, String>("starfleet", "enterprise", JacksonSupport.toJson(starship)))
    }

    producer.close()

    "JDBC Driver" should {
      "support wildcard selection" {
        val q = "SELECT * FROM `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING'"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 6
        rs.metaData.getColumnLabel(1) shouldBe "id"
        rs.metaData.getColumnLabel(2) shouldBe "time"
        rs.metaData.getColumnLabel(3) shouldBe "amount"
        rs.metaData.getColumnLabel(4) shouldBe "currency"
        rs.metaData.getColumnLabel(5) shouldBe "creditCardId"
        rs.metaData.getColumnLabel(6) shouldBe "merchantId"
      }
//      "support projections" {
//
//      }
//      "support where clauses"  {
//        val q = "SELECT * FROM `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING'  and currency= 'USD' LIMIT 1000"
//      }
//      "support limits"  {nn
//        val q = "SELECT * FROM `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING'  and currency= 'USD' LIMIT 1000"
//      }
      "return true for results" {
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        conn.createStatement().execute("select * from `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING' AND currency='USD'") shouldBe true
      }
      "return false if no results" {
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        conn.createStatement().execute("select * from `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING' AND currency='wibble'") shouldBe false
      }
    }
  }
}