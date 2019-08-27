package io.lenses.jdbc4

import io.kotlintest.matchers.collections.shouldContain
import io.kotlintest.matchers.collections.shouldContainAll
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.matchers.collections.shouldNotContain
import io.kotlintest.matchers.gte
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import io.lenses.jdbc4.resultset.resultSetList
import io.lenses.jdbc4.resultset.toList
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.sql.Connection
import java.sql.DatabaseMetaData

class LDatabaseMetaDataTest : WordSpec(), ProducerSetup {

  init {

    LensesDriver()

    val conn = conn()
    val topic1 = newTopicName()
    val topic2 = newTopicName()

    val taxiTopic = createTaxiTopic(conn)

    conn.createStatement().executeQuery("""
        CREATE TABLE $topic1 (_key int, id int, name string, quantity int, price double) FORMAT(INT, Avro) properties(partitions=3);
        CREATE TABLE $topic2 (_key int, id int, name string, quantity int, price double) FORMAT(INT, Json) properties(partitions=4);
      """.trimIndent())


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
      "return table types" {
        resultSetList(conn.metaData.tableTypes).map { it[0] } shouldBe listOf("SYSTEM", "USER")
      }
      "return all table names" {
        val tableNames = resultSetList(conn.metaData.getTables(null, null, null, null)).map { it[2] }
        tableNames.shouldContainAll(topic1, topic2)
      }
      "support table types when listing tables" {
        val tableNames = resultSetList(conn.metaData.getTables(null,
            null,
            null,
            arrayOf("USER"))).map { it[2].toString() }
        tableNames.shouldContain(topic1)
        tableNames.shouldContain(topic2)
        tableNames.shouldNotContain("__consumer_offsets")

        val systemTableNames = resultSetList(conn.metaData.getTables(null,
            null,
            null,
            arrayOf("SYSTEM"))).map { it[2].toString() }
        systemTableNames.shouldContain("__consumer_offsets")
        systemTableNames.shouldNotContain(topic1)
        systemTableNames.shouldNotContain(topic2)
      }
      "support table regex when listing tables" {
        // lets add some of our own tables and make sure they appear in the list of all
        val schema = SchemaBuilder.record("wibble").fields().requiredString("foo").endRecord()
        val producer = KafkaProducer<String, GenericData.Record>(producerProps())
        val record = GenericData.Record(schema)
        record.put("foo", "a")

        producer.send(ProducerRecord<String, GenericData.Record>("topicregex_dibble", "key1", record))
        producer.send(ProducerRecord<String, GenericData.Record>("topicregex_dobble", "key1", record))
        producer.send(ProducerRecord<String, GenericData.Record>("topicregex_dubble", "key1", record))
        producer.close()

        val tableNames = resultSetList(conn.metaData.getTables(null,
            null,
            "topicregex_d%",
            null)).map { it[2].toString() }
        tableNames.size shouldBe 3
        tableNames.shouldContainAll("topicregex_dibble", "topicregex_dobble", "topicregex_dubble")
      }
      "support listing columns with correct types" {
        val columns = conn.metaData.getColumns(null, null, null, null).toList()
        val currency = columns.filter { it[2] == taxiTopic }.first { it[3] == "VendorID" }
        currency[4] shouldBe java.sql.Types.OTHER
        currency[5] shouldBe "INT"

        val merchantId = columns.filter { it[2] == taxiTopic }.first { it[3] == "tpep_pickup_datetime" }
        merchantId[4] shouldBe java.sql.Types.OTHER
        merchantId[5] shouldBe "STRING"

        val blocked = columns.filter { it[2] == taxiTopic }.first { it[3] == "trip_distance" }
        blocked[4] shouldBe java.sql.Types.OTHER
        blocked[5] shouldBe "DOUBLE"
      }
      "support listing columns using table regex" {
        val columns = conn.metaData.getColumns(null, null, taxiTopic, null).toList()
        val currency = columns.filter { it[2] == taxiTopic }.first { it[3] == "VendorID" }
        currency[4] shouldBe java.sql.Types.OTHER
        currency[5] shouldBe "INT"

        val merchantId = columns.filter { it[2] == taxiTopic }.first { it[3] == "tpep_pickup_datetime" }
        merchantId[4] shouldBe java.sql.Types.OTHER
        merchantId[5] shouldBe "STRING"

        val blocked = columns.filter { it[2] == taxiTopic }.first { it[3] == "trip_distance" }
        blocked[4] shouldBe java.sql.Types.OTHER
        blocked[5] shouldBe "DOUBLE"
      }
      "support listing columns using column regex" {
        val columns = conn.metaData.getColumns(null, null, null, "VendorID").toList()
        val currency = columns.filter { it[2] == taxiTopic }.first { it[3] == "VendorID" }
        currency[4] shouldBe java.sql.Types.OTHER
        currency[5] shouldBe "INT"
      }
      "support listing columns using table and column regex" {
        val columns = conn.metaData.getColumns(null, null, taxiTopic, "VendorID").toList()
        val currency = columns.filter { it[2] == taxiTopic }.first { it[3] == "VendorID" }
        currency[4] shouldBe java.sql.Types.OTHER
        currency[5] shouldBe "INT"
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

  fun createTaxiTopic(conn: Connection): String {
    val topic = newTopicName()
    conn.createStatement().executeQuery("""
            create table $topic(
              _key string,
              VendorID int,
              tpep_pickup_datetime string,
              tpep_dropoff_datetime string,
              passenger_count int,
              trip_distance double, 
              pickup_longitude double,
              pickup_latitude double,
              RateCodeID int,
              store_and_fwd_flag string, 
              dropoff_longitude double,
              dropoff_latitude double,
              payment_type int,
              fare_amount double,
              extra double,
              mta_tax double,
              improvement_surcharge double,
              tip_amount double,
              tolls_amount double,
              total_amount double)
              format(string, avro)
        """.trimIndent()).toList().shouldHaveSize(1)
    return topic
  }
}