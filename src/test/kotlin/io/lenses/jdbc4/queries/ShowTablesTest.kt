package io.lenses.jdbc4.queries

import io.kotlintest.matchers.collections.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.LensesDriver
import io.lenses.jdbc4.ProducerSetup
import io.lenses.jdbc4.resultset.toList
import java.sql.DriverManager

class ShowTablesTest : FunSpec() , ProducerSetup {
  init {

    LensesDriver()

    val conn = DriverManager.getConnection("jdbc:lenses:kafka:http://localhost:24015", "admin", "admin")

    test("SHOW TABLES schema") {
      val q = "SHOW TABLES"
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(q)
      rs.metaData.columnCount shouldBe 4
      List(4) { rs.metaData.getColumnLabel(it + 1) } shouldBe listOf("name", "type", "partitions", "replicas")
    }

    test("SHOW TABLES data") {
      val topic1 = newTopicName()
      val topic2 = newTopicName()

      conn.createStatement().executeQuery("""
        CREATE TABLE $topic1 (_key int, id int, name string, quantity int, price double) FORMAT(INT, Avro) properties(partitions=3);
        CREATE TABLE $topic2 (_key int, id int, name string, quantity int, price double) FORMAT(INT, Json) properties(partitions=4);
      """.trimIndent())
      val q = "SHOW TABLES"
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(q).toList()
      rs.shouldContain(listOf(topic1, "USER", "3", "1"))
      rs.shouldContain(listOf(topic2, "USER", "4", "1"))
    }
  }
}