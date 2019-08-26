package io.lenses.jdbc4.queries

import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import io.lenses.jdbc4.LDriver
import io.lenses.jdbc4.ProducerSetup
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

data class Country(val name: String)

class SingleFieldSchemaQueryTest : WordSpec(), ProducerSetup {

  private val topic = "topic_" + UUID.randomUUID().toString().replace('-', '_')

  private fun populateCountries() {
    val countries = listOf(
        Country("Vanuatu"),
        Country("Comoros")
    )
    val schema = SchemaBuilder.record("country").fields().requiredString("name").endRecord()
    val producer = KafkaProducer<String, GenericData.Record>(producerProps())
    for (country in countries) {
      val record = GenericData.Record(schema)
      record.put("name", country.name)
      producer.send(ProducerRecord<String, GenericData.Record>(topic, country.name, record))
    }
  }

  init {

    LDriver()
    populateCountries()

    "JDBC Driver" should {
      "support wildcard for fixed schemas" {
        val q = "SELECT * FROM $topic"
        val stmt = conn().createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 1
        rs.metaData.getColumnLabel(1) shouldBe "unnamed"
      }
      "support projection for fixed schemas" {
        val q = "SELECT name FROM $topic"
        val stmt = conn().createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 1
        rs.metaData.getColumnLabel(1) shouldBe "name"
      }
    }
  }
}