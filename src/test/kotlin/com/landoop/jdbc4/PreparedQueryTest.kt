package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager

class PreparedQueryTest : WordSpec(), MovieData {

  init {

    LsqlDriver()

    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
    //val conn = DriverManager.getConnection("jdbc:lsql:kafka:https://master.lensesui.dev.landoop.com", "read", "read1")

    "JDBC Driver" should {
      "support prepared queries" {
        val sql = "select * from `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING' AND currency=?"
        val stmt = conn.prepareStatement(sql)
        stmt.execute(sql) shouldBe true
      }
    }
  }
}