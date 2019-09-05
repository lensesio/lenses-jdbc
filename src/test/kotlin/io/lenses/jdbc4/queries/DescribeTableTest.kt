package io.lenses.jdbc4.queries

import io.kotlintest.matchers.collections.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.LensesDriver
import io.lenses.jdbc4.ProducerSetup
import io.lenses.jdbc4.resultset.toList

class DescribeTableTest : FunSpec(), ProducerSetup {
  init {

    LensesDriver()
    val conn = conn()

    test("DESCRIBE TABLE with primitive for key/value") {
      val topic=newTopicName()
      conn.createStatement().executeUpdate("""
        CREATE TABLE $topic(_key string, value string) format(string, string)
      """.trimIndent())

      val q = "DESCRIBE TABLE $topic"
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(q)
      rs.metaData.columnCount shouldBe 2
      List(2) { rs.metaData.getColumnLabel(it + 1) } shouldBe listOf("key", "value")
    }

    test("DESCRIBE TABLE for Avro stored value") {
      val topic = newTopicName()
      conn.createStatement().executeQuery("""
        CREATE TABLE $topic (_key int, id int, name string, quantity int, price double) 
        FORMAT(INT, Avro);
      """.trimIndent())
      val q = "DESCRIBE TABLE $topic"
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(q).toList()
      rs.shouldContain(listOf("_value.id", "int"))
      rs.shouldContain(listOf("_value.name", "string"))
      rs.shouldContain(listOf("_value.quantity", "int"))
      rs.shouldContain(listOf("_value.price", "double"))
      rs.shouldContain(listOf("Value", "AVRO"))
      rs.shouldContain(listOf("Key", "INT"))
    }
  }
}