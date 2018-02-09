package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.sql.DriverManager

class InsertTest : WordSpec(), ProducerSetup {

  data class Starship(val name: String, val designation: String)

  fun populateStarships() {
    val starships = listOf(
        Starship("USS Enterprise", "1701D"),
        Starship("USS Discovery", "1031")
    )
    val producer = KafkaProducer<String, String>(props())
    for (starship in starships) {
      producer.send(ProducerRecord<String, String>("starfleet", starship.name, JacksonSupport.toJson(starship)))
    }
  }

  init {

    LsqlDriver()
    //   val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
    val conn = DriverManager.getConnection("jdbc:lsql:kafka:https://master.lensesui.dev.landoop.com", "read", "read1")

    "JDBC Driver" should {
      "support insertion" {
        val sql = "INSERT INTO topicA(field1,field2.field3) VALUES( v1,v2),(v3,v4)"
        val stmt = conn.createStatement()
        val rs = stmt.execute(sql)
      }
      "support prepared statements" {
        val sql = "INSERT INTO cc_data (customerFirstName, country, currency) values (?,?,?)"
        val stmt = conn.prepareStatement(sql)
        val rs = stmt.execute()
      }
      "return parameter info for prepared statements" {
        val sql = "INSERT INTO cc_data (customerFirstName, country, number) values (?,?,?)"
        val stmt = conn.prepareStatement(sql)
        val meta = stmt.parameterMetaData
        // this should be 3 even though the schema will have 6
        meta.parameterCount shouldBe 3
        meta.getParameterClassName(1) shouldBe "java.lang.String"
        meta.getParameterClassName(2) shouldBe "java.lang.String"
        meta.getParameterClassName(3) shouldBe "java.lang.String"
      }
    }
  }
}