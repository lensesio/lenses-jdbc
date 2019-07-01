package com.landoop.jdbc4

import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager

class PrecisionQueryTest : WordSpec(), EquitiesData {

  init {

    LDriver()
    val topic = try {
      populateEquities()
    } catch (e: Throwable) {
      e.printStackTrace()
      throw e
    }

    val q = "SELECT * FROM $topic"
    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
    val stmt = conn.createStatement()

    "JDBC Driver" should {
      "use unlimited precision for strings" {
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 6
        rs.metaData.getColumnLabel(1) shouldBe "ticker"
        rs.metaData.getColumnLabel(2) shouldBe "name"
        rs.metaData.getColumnLabel(3) shouldBe "price"
        rs.metaData.getColumnLabel(4) shouldBe "float"
        rs.metaData.getColumnLabel(5) shouldBe "sector"
        rs.metaData.getColumnLabel(6) shouldBe "yield"
        rs.next()
        // our strings are unlimited
        rs.metaData.getPrecision(2) shouldBe Int.MAX_VALUE
      }
      "support precision for logical decimals backed by bytes" {
        val rs = stmt.executeQuery(q)
        rs.next()
        rs.metaData.getPrecision(3) shouldBe 10
      }
      "support precision for fixed types" {
        val rs = stmt.executeQuery(q)
        rs.next()
        rs.metaData.getPrecision(1) shouldBe 4
      }
      "return 0 for other numerical types" {
        val rs = stmt.executeQuery(q)
        rs.next()
        rs.metaData.getPrecision(4) shouldBe 0
        rs.metaData.getPrecision(6) shouldBe 0
      }
    }
  }
}