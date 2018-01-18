package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager

class LsqlQueryIntTest : WordSpec() {

  init {

    LsqlDriver()

    "JDBC Driver" should {
      "support wildcard selection" {
        val q = "SELECT * FROM `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING'"
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        val rs = conn.createStatement().execute(q)
      }
      "support projections" {

      }
      "support where clauses" should {
        val q = "SELECT * FROM `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING'  and currency= 'USD' LIMIT 1000"
      }
      "support limits" should {
        val q = "SELECT * FROM `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING'  and currency= 'USD' LIMIT 1000"
      }
      "return true for results" {
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        conn.createStatement().execute("select * from `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING' AND currency='USD'") shouldBe true
      }
      "return false if no results" {
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        conn.createStatement().execute("select * from `cc_payments` WHERE _vtype='AVRO' AND _ktype='STRING' AND currency='wibble'") shouldBe false
      }
    }
  }
}