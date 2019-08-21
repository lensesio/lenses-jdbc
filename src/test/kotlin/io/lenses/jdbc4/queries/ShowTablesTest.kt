package io.lenses.jdbc4.queries

import io.kotlintest.matchers.collections.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.LDriver
import io.lenses.jdbc4.resultset.toList
import java.sql.DriverManager

class ShowTablesTest : FunSpec() {
  init {

    LDriver()

    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:24015", "admin", "admin999")

    test("SHOW TABLES schema") {
      val q = "SHOW TABLES"
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(q)
      rs.metaData.columnCount shouldBe 4
      List(4) { rs.metaData.getColumnLabel(it + 1) } shouldBe listOf("name", "type", "partitions", "replicas")
    }

    test("SHOW TABLES data") {
      val q = "SHOW TABLES"
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(q).toList()
      rs.shouldContain(listOf("sea_vessel_position_reports", "USER", "3", "1"))
      rs.shouldContain(listOf("telecom_italia_data", "USER", "4", "1"))
    }
  }
}