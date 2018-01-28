package com.landoop.jdbc4

import io.kotlintest.matchers.contain
import io.kotlintest.matchers.containsAll
import io.kotlintest.matchers.gte
import io.kotlintest.matchers.should
import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldNot
import io.kotlintest.specs.WordSpec
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.sql.DatabaseMetaData
import java.sql.DriverManager

class LsqlDatabaseMetaDataTest : WordSpec(), ProducerSetup {

  init {

    LsqlDriver()
    // val conn = DriverManager.getConnection("jdbc:lsql:kafka:https://master.lensesui.dev.landoop.com", "read", "read1")
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
        val tableNames = resultSetList(conn.metaData.getTables(null, null, null, null)).map { it[2].toString() }
        tableNames should containsAll("cc_data", "cc_payments")
      }
      "support table types when listing tables" {
        val tableNames = resultSetList(conn.metaData.getTables(null, null, null, arrayOf("TABLE"))).map { it[2].toString() }
        tableNames should contain("cc_data")
        tableNames should contain("cc_payments")
        tableNames shouldNot contain("__consumer_offsets")
        tableNames shouldNot contain("_schemas")

        val systemTableNames = resultSetList(conn.metaData.getTables(null, null, null, arrayOf("SYSTEM TABLE"))).map { it[2].toString() }
        systemTableNames should containsAll("__consumer_offsets", "_schemas", "_kafka_lenses_processors")
        systemTableNames shouldNot contain("cc_data")
        systemTableNames shouldNot contain("cc_payments")
      }
      "support table regex when listing tables" {
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
        tableNames.size shouldBe 3
        tableNames should containsAll("topic_dibble", "topic_dobble", "topic_dubble")
      }
      "support listing columns with correct types" {
        val columns = resultSetList(conn.metaData.getColumns(null, null, null, null))
        println(columns)
        val currency = columns.filter { it[2] == "cc_payments" }.first { it[3] == "currency" }
        currency[4] shouldBe java.sql.Types.VARCHAR
        currency[5] shouldBe "STRING"

        val merchantId = columns.filter { it[2] == "cc_payments" }.first { it[3] == "merchantId" }
        merchantId[4] shouldBe java.sql.Types.BIGINT
        merchantId[5] shouldBe "LONG"

        val inputs = columns.filter { it[2] == "bitcoin_transactions" }.first { it[3] == "inputs" }
        inputs[4] shouldBe java.sql.Types.ARRAY
        inputs[5] shouldBe "ARRAY"

        val blocked = columns.filter { it[2] == "cc_data" }.first { it[3] == "blocked" }
        blocked[4] shouldBe java.sql.Types.BOOLEAN
        blocked[5] shouldBe "BOOLEAN"
      }
      "support listing columns with correct nullability" {
        val columns = resultSetList(conn.metaData.getColumns(null, null, null, null))
        println(columns)
        val currency = columns.filter { it[2] == "cc_payments" }.first { it[3] == "currency" }
        currency[10] shouldBe DatabaseMetaData.columnNoNulls
        currency[17] shouldBe "NO"

        val inputs = columns.filter { it[2] == "bitcoin_transactions" }.first { it[3] == "inputs" }
        inputs[10] shouldBe DatabaseMetaData.columnNullable
        inputs[17] shouldBe "YES"
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
      "not support batch updates" {
        conn.metaData.supportsBatchUpdates() shouldBe false
      }
      "be read only" {
        conn.metaData.isReadOnly() shouldBe true
      }
    }
  }
}