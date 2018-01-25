package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.sql.DriverManager

class PrecisionQueryTest : WordSpec(), QuerySetup {

  fun populateCountries() {
    val countries = listOf(
        Country("Vanuatu"),
        Country("Comoros")
    )
    val producer = KafkaProducer<String, String>(props())
    for (country in countries) {
      producer.send(ProducerRecord<String, String>("country", country.name, country.name))
    }
  }

  init {

    LsqlDriver()
    populateCountries()

    "JDBC Driver" should {
      "support precision for fixed types" {
        val q = "SELECT * FROM country"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 1
        rs.metaData.getColumnLabel(1) shouldBe "name"
        conn.close()
      }
      "support precision for logical decimals" {
        val q = "SELECT name FROM country"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 1
        rs.metaData.getColumnLabel(1) shouldBe "name"
        conn.close()
      }
    }
  }
}