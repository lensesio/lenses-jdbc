package com.landoop.jdbc4

import io.kotlintest.shouldThrow
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager

class LsqlStatementTest : WordSpec() {
  init {
    LsqlDriver()

    "LsqlStatement" should {
      "throw not supported exception for execute with auto generated columns" {
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        shouldThrow<Exception> {
          conn.createStatement().execute("select * from table", 1)
        }
        shouldThrow<Exception> {
          conn.createStatement().execute("select * from table", intArrayOf(1))
        }
        shouldThrow<Exception> {
          conn.createStatement().execute("select * from table", arrayOf("a"))
        }
      }
      "support multiple resultsets" {

      }
    }
  }
}