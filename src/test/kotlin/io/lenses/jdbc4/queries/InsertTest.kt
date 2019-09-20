package io.lenses.jdbc4.queries

import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.LensesDriver
import io.lenses.jdbc4.ProducerSetup
import io.lenses.jdbc4.resultset.toList

class InsertTest : FunSpec(), ProducerSetup {
  init {

    LensesDriver()

    val conn = conn()

    test("INSERT into table test") {

      val tableName = "testtable__" + System.currentTimeMillis()

      val stmt1 = conn.createStatement()
      val rs = stmt1.executeQuery("CREATE TABLE $tableName (a text, b int, c boolean) FORMAT (json, json)")
      rs.metaData.columnCount shouldBe 2
      List(2) { rs.metaData.getColumnLabel(it + 1) } shouldBe listOf("flag", "info")

      conn.createStatement().executeUpdate("INSERT INTO $tableName (a,b,c) VALUES('hello', 2, true)")
      conn.createStatement().executeUpdate("INSERT INTO $tableName (a,b,c) VALUES('world', 5, false)")
      stmt1.executeQuery("SELECT * FROM $tableName").toList().shouldHaveSize(2)
    }
  }
}