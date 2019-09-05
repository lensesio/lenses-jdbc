package io.lenses.jdbc4

import io.kotlintest.shouldThrow
import io.kotlintest.specs.ShouldSpec
import java.sql.SQLFeatureNotSupportedException
import java.sql.Statement

class LStatementTest : ShouldSpec(), ProducerSetup {
  init {

    val conn = conn()

    should("throw exception for execute with auto generated columns") {
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

    should("throw exception for transaction methods") {
      shouldThrow<SQLFeatureNotSupportedException> {
        conn.rollback()
      }
      shouldThrow<SQLFeatureNotSupportedException> {
        conn.commit()
      }
      shouldThrow<SQLFeatureNotSupportedException> {
        conn.setSavepoint()
      }
    }

    should("throw exception for blob methods") {
      shouldThrow<SQLFeatureNotSupportedException> {
        conn.createBlob()
      }
      shouldThrow<SQLFeatureNotSupportedException> {
        conn.createClob()
      }
      shouldThrow<SQLFeatureNotSupportedException> {
        conn.createNClob()
      }
    }
  }
}