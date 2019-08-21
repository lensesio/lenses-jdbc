package io.lenses.jdbc4

import io.kotlintest.shouldThrow
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager
import java.sql.SQLFeatureNotSupportedException
import java.sql.Statement

class LStatementTest : WordSpec() {
  init {
    io.lenses.jdbc4.LDriver()

    "LsqlStatement" should {
      "throw exception for execute with auto generated columns" {
        val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
        shouldThrow<SQLFeatureNotSupportedException> {
          conn.createStatement().execute("select * from table", Statement.RETURN_GENERATED_KEYS)
        }
        shouldThrow<SQLFeatureNotSupportedException> {
          conn.createStatement().execute("select * from table", intArrayOf(1))
        }
        shouldThrow<SQLFeatureNotSupportedException> {
          conn.createStatement().execute("select * from table", arrayOf("a"))
        }
      }
    }
  }
}