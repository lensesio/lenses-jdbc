package io.lenses.jdbc4.queries

import io.kotlintest.matchers.collections.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.LDriver
import io.lenses.jdbc4.resultset.toList
import java.sql.DriverManager

class ShowVirtualTablesTest : FunSpec() {
  init {

    LDriver()

    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:24015", "admin", "admin999")

    test("SHOW VIRTUAL TABLES schema") {
      val q = "SHOW VIRTUAL TABLES"
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(q)
      rs.metaData.columnCount shouldBe 2
      List(2) { rs.metaData.getColumnLabel(it + 1) } shouldBe listOf("name", "description")
    }

    test("SHOW VIRTUAL TABLES data") {
      val q = "SHOW VIRTUAL TABLES"
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(q).toList()
      println(rs)
      rs.shouldContain(listOf("__dual", "A virtual table with a single row and field"))
      rs.shouldContain(listOf("__fields", "Lists all fields known to this connection"))
    }
  }
}