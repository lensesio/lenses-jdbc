package com.landoop.jdbc4

import io.kotlintest.matchers.collections.shouldContain
import io.kotlintest.matchers.collections.shouldContainAll
import io.kotlintest.matchers.collections.shouldNotContain
import io.kotlintest.matchers.gte
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.DriverManager

class LsqlDatabaseMetaDataTest : WordSpec(), ProducerSetup {

  init {

    LsqlDriver()

    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")

    "LsqlDatabaseMetaDataTest" should {
      "declare support for multiple result sets" {
        conn.metaData.supportsMultipleResultSets() shouldBe true
        conn.metaData.supportsMultipleOpenResults() shouldBe false
        conn.metaData.supportsMultipleTransactions() shouldBe false
      }
      "declare support for joins" {
        conn.metaData.supportsFullOuterJoins() shouldBe false
        conn.metaData.supportsLimitedOuterJoins() shouldBe false
        conn.metaData.supportsOuterJoins() shouldBe false
      }
      "declare support for subqueries" {
        conn.metaData.supportsSubqueriesInIns() shouldBe true
        conn.metaData.supportsCorrelatedSubqueries() shouldBe false
        conn.metaData.supportsSubqueriesInComparisons() shouldBe false
        conn.metaData.supportsSubqueriesInExists() shouldBe false
        conn.metaData.supportsSubqueriesInQuantifieds() shouldBe false
      }
      "declare support for transactions" {
        conn.metaData.supportsTransactions() shouldBe false
        conn.metaData.supportsMultipleTransactions() shouldBe false
        conn.metaData.dataDefinitionIgnoredInTransactions() shouldBe false
        conn.metaData.defaultTransactionIsolation shouldBe Connection.TRANSACTION_NONE
        conn.metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED) shouldBe false
        conn.metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ) shouldBe false
        conn.metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE) shouldBe false
        conn.metaData.supportsDataDefinitionAndDataManipulationTransactions() shouldBe false
        conn.metaData.supportsDataManipulationTransactionsOnly() shouldBe false
      }
      "return type info" {

        val string = resultSetList(conn.metaData.typeInfo).first { it[0] == "STRING" }
        string[1] shouldBe java.sql.Types.VARCHAR
        string[6] shouldBe DatabaseMetaData.typeNullable
        string[3] shouldBe '"'
        string[4] shouldBe '"'

        val long = resultSetList(conn.metaData.typeInfo).first { it[0] == "LONG" }
        long[1] shouldBe java.sql.Types.BIGINT
        long[6] shouldBe DatabaseMetaData.typeNullable
        long[3] shouldBe null
        long[4] shouldBe null
      }
      "return compatible table types" {
        resultSetList(conn.metaData.tableTypes).map { it[0] } shouldBe listOf("TABLE", "SYSTEM TABLE")
      }
      "return all table names" {
        val tableNames = resultSetList(conn.metaData.getTables(null, null, null, null)).map { it[2].toString() }
        tableNames.shouldContainAll("cc_data", "cc_payments")
      }
      "support table types when listing tables" {
        val tableNames = resultSetList(conn.metaData.getTables(null, null, null, arrayOf("TABLE"))).map { it[2].toString() }
        tableNames.shouldContain("cc_data")
        tableNames.shouldContain("cc_payments")
        tableNames.shouldNotContain("__consumer_offsets")
        tableNames.shouldNotContain("_kafka_lenses_processors")

        val systemTableNames = resultSetList(conn.metaData.getTables(null, null, null, arrayOf("SYSTEM TABLE"))).map { it[2].toString() }
        systemTableNames.shouldContainAll("__consumer_offsets", "_schemas", "_kafka_lenses_processors")
        systemTableNames.shouldNotContain("cc_data")
        systemTableNames.shouldNotContain("cc_payments")
      }
      "!support table regex when listing tables" {
        // lets add some of our own tables and make sure they appear in the list of all
        val schema = SchemaBuilder.record("wibble").fields().requiredString("foo").endRecord()
        val producer = KafkaProducer<String, GenericData.Record>(producerProps())
        val record = GenericData.Record(schema)
        record.put("foo", "a")

        producer.send(ProducerRecord<String, GenericData.Record>("topicregex_dibble", "key1", record))
        producer.send(ProducerRecord<String, GenericData.Record>("topicregex_dobble", "key1", record))
        producer.send(ProducerRecord<String, GenericData.Record>("topicregex_dubble", "key1", record))
        producer.close()

        val tableNames = resultSetList(conn.metaData.getTables(null, null, "topicregex_d%", null)).map { it[2].toString() }
        tableNames.size shouldBe 3
        tableNames.shouldContainAll("topicregex_dibble", "topicregex_dobble", "topicregex_dubble")
      }
      "support listing columns with correct types" {
        val columns = resultSetList(conn.metaData.getColumns(null, null, null, null))
        val currency = columns.filter { it[2] == "cc_payments" }.first { it[3] == "currency" }
        currency[4] shouldBe java.sql.Types.VARCHAR
        currency[5] shouldBe "STRING"

        val merchantId = columns.filter { it[2] == "cc_payments" }.first { it[3] == "merchantId" }
        merchantId[4] shouldBe java.sql.Types.BIGINT
        merchantId[5] shouldBe "LONG"

        val blocked = columns.filter { it[2] == "cc_data" }.first { it[3] == "blocked" }
        blocked[4] shouldBe java.sql.Types.BOOLEAN
        blocked[5] shouldBe "BOOLEAN"
      }
      "!support listing columns with correct nullability" {

        val topic = createTopic()

        val schema = SchemaBuilder.record("dabble").fields()
            .optionalDouble("optdouble")
            .optionalBoolean("optbool")
            .requiredString("reqstring")
            .requiredLong("reqlong")
            .endRecord()

        registerValueSchema(topic, schema)

        val producer = KafkaProducer<String, GenericData.Record>(producerProps())
        val record = GenericData.Record(schema)
        record.put("optdouble", 123.4)
        record.put("optbool", true)
        record.put("reqstring", "a")
        record.put("reqlong", 555L)

        producer.send(ProducerRecord<String, GenericData.Record>(topic, "key1", record))
        producer.close()

        val columns = resultSetList(conn.metaData.getColumns(null, null, topic, null))
        println(columns)
        val reqstring = columns.first { it[3] == "reqstring" }
        reqstring[10] shouldBe DatabaseMetaData.columnNoNulls
        reqstring[17] shouldBe "NO"

        val reqlong = columns.first { it[3] == "reqlong" }
        reqlong[10] shouldBe DatabaseMetaData.columnNoNulls
        reqlong[17] shouldBe "NO"

        val optdouble = columns.first { it[3] == "optdouble" }
        optdouble[10] shouldBe DatabaseMetaData.columnNullable
        optdouble[17] shouldBe "YES"

        val optbool = columns.first { it[3] == "optbool" }
        optbool[10] shouldBe DatabaseMetaData.columnNullable
        optbool[17] shouldBe "YES"
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
        conn.metaData.isReadOnly shouldBe true
      }
    }
  }
}