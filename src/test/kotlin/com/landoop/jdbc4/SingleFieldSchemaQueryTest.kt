package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.sql.DriverManager
import java.util.*

data class Country(val name: String)

class SingleFieldSchemaQueryTest : WordSpec(), ProducerSetup {

  val topic = "topic_" + UUID.randomUUID().toString().replace('-', '_')

  fun populateCountries() {

    val countries = listOf(
        Country("Vanuatu"),
        Country("Comoros")
    )

    val schema = SchemaBuilder.record("country").fields().requiredString("name").endRecord()

    val producer = KafkaProducer<String, GenericData.Record>(props())
    for (country in countries) {
      val record = GenericData.Record(schema)
      record.put("name", country.name)
      producer.send(ProducerRecord<String, GenericData.Record>(topic, country.name, record))
    }

  }

  init {

    LsqlDriver()
    populateCountries()

    "JDBC Driver" should {
      "support wildcard for fixed schemas" {
        val q = "SELECT * FROM $topic"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 1
        rs.metaData.getColumnLabel(1) shouldBe "name"
      }
      "support projection for fixed schemas" {
        val q = "SELECT name FROM $topic"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 1
        rs.metaData.getColumnLabel(1) shouldBe "name"
      }
      "return data for fixed schema" {
        val q = "SELECT * FROM $topic"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val stmt = conn.createStatement()
        stmt.execute(q) shouldBe true
        val rs = stmt.resultSet
        rs.next()
        rs.getString(1) shouldBe "Vanuatu"
        rs.next()
        rs.getString(1) shouldBe "Comoros"
      }
    }
  }
}