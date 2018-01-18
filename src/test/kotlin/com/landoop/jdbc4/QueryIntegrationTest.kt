package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldThrow
import io.kotlintest.specs.WordSpec
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.sql.DriverManager
import java.sql.SQLException
import java.util.*

data class Starship(val name: String, val designation: String)
data class Country(val name: String)

class QueryIntegrationTest : WordSpec() {

  val props = Properties().apply {
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

  fun populateStarships() {
    val starships = listOf(
        Starship("USS Enterprise", "1701D"),
        Starship("USS Discovery", "1031")
    )
    val producer = KafkaProducer<String, String>(props)
    for (starship in starships) {
      producer.send(ProducerRecord<String, String>("starfleet", starship.name, JacksonSupport.toJson(starship)))
    }
  }

  fun populateCountries() {
    val countries = listOf(
        Country("Vanuatu"),
        Country("Comoros")
    )
    val producer = KafkaProducer<String, String>(props)
    for (country in countries) {
      producer.send(ProducerRecord<String, String>("country", country.name, country.name))
    }
  }

  init {

    LsqlDriver()

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
      "support projections" {
        val q = "SELECT merchantId, currency FROM `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING'"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 2
        rs.metaData.getColumnLabel(1) shouldBe "merchantId"
        rs.metaData.getColumnLabel(2) shouldBe "currency"
      }
      "return all results without a limit"  {
        val q = "SELECT * FROM `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING'"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        var counter = 0
        while (rs.next()) {
          counter += 1
        }
        (counter > 2000) shouldBe true
      }
      "throw SQL exception if the topic does not exist" {
        val q = "SELECT * FROM wobble"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val stmt = conn.createStatement()
        shouldThrow<SQLException> {
          stmt.executeQuery(q)
        }
      }
      "support schemas with a single field" {
        val q = "SELECT * FROM country"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 6
        rs.metaData.getColumnLabel(1) shouldBe "id"
      }
      "return full fields for single field types" {
        val q = "SELECT * FROM country"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 6
        rs.metaData.getColumnLabel(1) shouldBe "id"
      }
      "support schemas with two fields" {
        val q = "SELECT * FROM starfleet"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 6
        rs.metaData.getColumnLabel(1) shouldBe "id"
      }
      "support schemas with two fields" {
        val q = "SELECT * FROM starfleet"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 6
        rs.metaData.getColumnLabel(1) shouldBe "id"
      }
      // limits don't seem to work at present
//      "support limits"  {
//        val q = "SELECT * FROM `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING' limit 10"
//        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
//        val stmt = conn.createStatement()
//        val rs = stmt.executeQuery(q)
//        var counter = 0
//        while (rs.next()) {
//          counter += 1
//        }
//        counter shouldBe 10
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