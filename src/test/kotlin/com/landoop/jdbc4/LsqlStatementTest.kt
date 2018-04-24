package com.landoop.jdbc4

import io.kotlintest.shouldThrow
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager
import java.sql.Statement

class LsqlStatementTest : WordSpec() {
  init {
    LsqlDriver()

    "LsqlStatement" should {
      "throw exception for execute with auto generated columns" {
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        shouldThrow<Exception> {
          conn.createStatement().execute("select * from table", Statement.RETURN_GENERATED_KEYS)
        }
        shouldThrow<Exception> {
          conn.createStatement().execute("select * from table", intArrayOf(1))
        }
        shouldThrow<Exception> {
          conn.createStatement().execute("select * from table", arrayOf("a"))
        }
      }
    }
  }
}