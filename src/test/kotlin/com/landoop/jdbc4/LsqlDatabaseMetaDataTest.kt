package com.landoop.jdbc4

import io.kotlintest.matchers.containsAll
import io.kotlintest.matchers.gte
import io.kotlintest.matchers.should
import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.sql.DriverManager

class LsqlDatabaseMetaDataTest : WordSpec(), ProducerSetup {

  init {

    LsqlDriver()
    //val conn = DriverManager.getConnection("jdbc:lsql:kafka:https://master.lensesui.dev.landoop.com", "read", "read1")
    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")

    "LsqlDatabaseMetaDataTest" should {
      "declare support for multiple result sets" {
        conn.metaData.supportsMultipleResultSets() shouldBe true
        conn.metaData.supportsMultipleOpenResults() shouldBe false
        conn.metaData.supportsMultipleTransactions() shouldBe false
      }
      "return compatible table types" {
        resultSetList(conn.metaData.tableTypes).map { it[0] } shouldBe listOf("TABLE", "SYSTEM TABLE")
      }
      "return all table names" {

        // lets add some of our own tables and make sure they appear in the list of all
        val schema = SchemaBuilder.record("wibble").fields().requiredString("foo").endRecord()
        val producer = KafkaProducer<String, GenericData.Record>(props())
        val record = GenericData.Record(schema)
        record.put("foo", "a")

        producer.send(ProducerRecord<String, GenericData.Record>("topic_dibble", "key1", record))
        producer.send(ProducerRecord<String, GenericData.Record>("topic_dobble", "key1", record))
        producer.send(ProducerRecord<String, GenericData.Record>("topic_dubble", "key1", record))
        producer.close()

        val tableNames = resultSetList(conn.metaData.getTables(null, null, null, null)).map { it[2].toString() }
        tableNames should containsAll("cc_data", "topic_dibble", "topic_dobble", "topic_dubble")
      }
      "support table regex when listing table names" {

        // lets add some of our own tables and make sure they appear in the list of all
        val schema = SchemaBuilder.record("wibble").fields().requiredString("foo").endRecord()
        val producer = KafkaProducer<String, GenericData.Record>(props())
        val record = GenericData.Record(schema)
        record.put("foo", "a")

        producer.send(ProducerRecord<String, GenericData.Record>("topic_dibble", "key1", record))
        producer.send(ProducerRecord<String, GenericData.Record>("topic_dobble", "key1", record))
        producer.send(ProducerRecord<String, GenericData.Record>("topic_dubble", "key1", record))
        producer.close()

        val tableNames = resultSetList(conn.metaData.getTables(null, null, "topic_d%", null)).map { it[2].toString() }
        tableNames should containsAll("topic_dibble", "topic_dobble", "topic_dubble")
      }
      "return versioning information" {
        conn.metaData.databaseMajorVersion shouldBe gte(1)
        conn.metaData.databaseMinorVersion shouldBe gte(1)
        conn.metaData.driverMajorVersion shouldBe 0
        conn.metaData.driverMinorVersion shouldBe gte(1)
        conn.metaData.databaseProductName shouldBe Constants.ProductName
        conn.metaData.jdbcMajorVersion shouldBe 4
        conn.metaData.jdbcMinorVersion shouldBe 0
      }
    }
  }
}