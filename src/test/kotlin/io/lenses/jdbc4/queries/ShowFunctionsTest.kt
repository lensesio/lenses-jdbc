package io.lenses.jdbc4.queries

import io.kotlintest.matchers.collections.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.LDriver
import io.lenses.jdbc4.resultset.toList
import java.sql.DriverManager

class ShowFunctionsTest : FunSpec() {
  init {

    LDriver()

    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:24015", "admin", "admin999")

    test("SHOW FUNCTIONS schema") {
      val q = "SHOW FUNCTIONS"
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(q)
      rs.metaData.columnCount shouldBe 3
      List(3) { rs.metaData.getColumnLabel(it + 1) } shouldBe listOf("name", "description", "return_type")
    }

    test("SHOW FUNCTIONS data") {
      val q = "SHOW FUNCTIONS"
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(q).toList()
      println(rs)
    }
  }
}