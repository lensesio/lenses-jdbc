package com.landoop.jdbc4

import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager

class PrecisionQueryTest : WordSpec(), EquitiesData {

  init {

    LsqlDriver()
    val topic = populateEquities()

    val q = "SELECT * FROM $topic"
    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
    val stmt = conn.createStatement()

    "JDBC Driver" should {
      "support precision for strings" {
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 8
        rs.metaData.getColumnLabel(1) shouldBe "ticker"
        rs.metaData.getColumnLabel(2) shouldBe "amount"
        rs.metaData.getColumnLabel(3) shouldBe "foo"
        rs.metaData.getColumnLabel(4) shouldBe "boolean"
        rs.metaData.getColumnLabel(5) shouldBe "bytes"
        rs.metaData.getColumnLabel(6) shouldBe "double"
        rs.metaData.getColumnLabel(7) shouldBe "int"
        rs.metaData.getColumnLabel(8) shouldBe "long"
        rs.next()
        // our strings are unlimited
        rs.metaData.getPrecision(1) shouldBe Int.MAX_VALUE
      }
      "support precision for logical decimals backed by bytes" {
        val rs = stmt.executeQuery(q)
        rs.next()
        rs.metaData.getPrecision(2) shouldBe 10
      }
      "support precision for fixed types" {
        val rs = stmt.executeQuery(q)
        rs.next()
        rs.metaData.getPrecision(3) shouldBe 5
      }
      "return 0 for all other types" {
        val rs = stmt.executeQuery(q)
        rs.next()
        rs.metaData.getPrecision(4) shouldBe 0
        rs.metaData.getPrecision(5) shouldBe 0
        rs.metaData.getPrecision(6) shouldBe 0
        rs.metaData.getPrecision(7) shouldBe 0
        rs.metaData.getPrecision(8) shouldBe 0
      }
    }
  }
}