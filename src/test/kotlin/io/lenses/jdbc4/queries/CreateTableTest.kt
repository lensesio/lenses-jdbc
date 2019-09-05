package io.lenses.jdbc4.queries

import io.kotlintest.matchers.collections.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.LensesDriver
import io.lenses.jdbc4.resultset.toList
import java.sql.DriverManager

class CreateTableTest : FunSpec() {
  init {

    LensesDriver()

    val conn = DriverManager.getConnection("jdbc:lenses:kafka:http://localhost:24015", "admin", "admin")

    test("CREATE TABLE foo") {

      val tableName = "testtable__" + System.currentTimeMillis()

      val stmt1 = conn.createStatement()
      val rs = stmt1.executeQuery("CREATE TABLE $tableName (a text, b int, c boolean) FORMAT (json, json)")
      rs.metaData.columnCount shouldBe 2
      List(2) { rs.metaData.getColumnLabel(it + 1) } shouldBe listOf("flag", "info")

      val stmt2 = conn.createStatement()
      val rs2 = stmt2.executeQuery("SHOW TABLES").toList()
      rs2.shouldContain(listOf(tableName, "USER", "1", "1"))
    }
  }
}