package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldThrow
import io.kotlintest.mock.mock
import io.kotlintest.specs.WordSpec
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import java.sql.ResultSet
import java.sql.SQLFeatureNotSupportedException
import java.sql.Statement

class RowResultSetTest : WordSpec() {
  init {
    "LsqlResultSet" should {
      "support findColumn as 1-indexed" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").optionalString("goo").endRecord()
        val records = listOf(RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()))
        val rs = RowResultSet(null, schema, records)
        rs.next()
        rs.findColumn("foo") shouldBe 1
        rs.findColumn("goo") shouldBe 2
      }
      "use next to iterate records" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(
            RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "boo").build())
        )
        val rs = RowResultSet(null, schema, records)
        rs.next()
        rs.getString(1) shouldBe "woo"
        rs.next()
        rs.getString(1) shouldBe "boo"
      }
      "use previous to iterate records" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(
            RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "boo").build())
        )
        val rs = RowResultSet(null, schema, records)
        rs.last()
        rs.getString(1) shouldBe "boo"
        rs.previous()
        rs.getString(1) shouldBe "woo"
      }
      "support by label lookup" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").optionalString("goo").endRecord()
        val records = listOf(RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()))
        val rs = RowResultSet(null, schema, records)
        rs.next()
        rs.getString("foo") shouldBe "woo"
        rs.getString("goo") shouldBe null
      }
      "getString of null should return null" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(RecordRow(GenericRecordBuilder(schema).set("foo", null).build()))
        val rs = RowResultSet(null, schema, records)
        rs.next()
        rs.getString("foo") shouldBe null
      }
      "support absolute positive position" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(
            RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "goo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "boo").build())
        )
        val rs = RowResultSet(null, schema, records)
        rs.absolute(3)
        rs.getString(1) shouldBe "boo"
        rs.absolute(1)
        rs.getString(1) shouldBe "woo"
      }
      "support absolute negative position" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(
            RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "boo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "goo").build())
        )
        val rs = RowResultSet(null, schema, records)
        // -1 is defined as the last result
        rs.absolute(-1)
        rs.getString(1) shouldBe "goo"
        rs.absolute(-3)
        rs.getString(1) shouldBe "woo"

        shouldThrow<IndexOutOfBoundsException> {
          rs.absolute(-4)
        }
      }
      "support relative positioning" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(
            RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "boo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "goo").build())
        )
        val rs = RowResultSet(null, schema, records)
        // adding 2 should move us onto the second result
        rs.relative(2)
        rs.getString(1) shouldBe "boo"
        rs.relative(1)
        rs.getString(1) shouldBe "goo"
      }
      "support first() and isFirst()" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(
            RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "boo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "goo").build())
        )
        val rs = RowResultSet(null, schema, records)
        rs.isFirst shouldBe false
        rs.next()
        rs.isFirst shouldBe true
        rs.next()
        rs.isFirst shouldBe false
        rs.first()
        rs.isFirst shouldBe true
      }
      "support last() and isLast()" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(
            RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "boo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "goo").build())
        )
        val rs = RowResultSet(null, schema, records)
        rs.isLast shouldBe false
        rs.last()
        rs.isLast shouldBe true
        rs.previous()
        rs.isLast shouldBe false
      }
      "support isAfterLast()" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(
            RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "boo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", "goo").build())
        )
        val rs = RowResultSet(null, schema, records)
        rs.isAfterLast shouldBe false
        rs.last()
        rs.isAfterLast shouldBe false
        rs.next()
        rs.isAfterLast shouldBe true
      }
      "support beforeFirst and isBeforeFirst()" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(
            RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", null).build())
        )
        val rs = RowResultSet(null, schema, records)
        // should start as before first
        rs.isBeforeFirst shouldBe true
        // move on past the marker
        rs.next()
        rs.isBeforeFirst shouldBe false
        // back to before first again
        rs.beforeFirst()
        rs.isBeforeFirst shouldBe true
      }
      "return READ ONLY for concurrency" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()))
        RowResultSet(null, schema, records).concurrency shouldBe ResultSet.CONCUR_READ_ONLY
      }
      "return true for isClosed" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()))
        RowResultSet(null, schema, records).isClosed shouldBe true
      }
      "return TYPE_SCROLL_INSENSITIVE for type" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()))
        RowResultSet(null, schema, records).type shouldBe ResultSet.TYPE_SCROLL_INSENSITIVE
      }
      "return -1 for fetch size" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()))
        RowResultSet(null, schema, records).fetchSize shouldBe -1
      }
      "return the statement used to create the resultset" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()))
        val stmt = mock<Statement>()
        RowResultSet(stmt, schema, records).statement shouldBe stmt
      }
      "be a wrapper for RowResultSet" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()))
        val stmt = mock<Statement>()
        RowResultSet(stmt, schema, records).isWrapperFor(RowResultSet::class.java) shouldBe true
      }
      "track last value to support wasNull" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(
            RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()),
            RecordRow(GenericRecordBuilder(schema).set("foo", null).build())
        )
        val rs = RowResultSet(null, schema, records)
        rs.next()
        rs.getString("foo") shouldBe "woo"
        rs.wasNull() shouldBe false

        rs.next()
        rs.getString("foo") shouldBe null
        rs.wasNull() shouldBe true
      }
      "throw SQLFeatureNotSupportedException for deletion methods" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()))
        val rs = RowResultSet(null, schema, records)

        shouldThrow<SQLFeatureNotSupportedException> {
          rs.deleteRow()
        }

        shouldThrow<SQLFeatureNotSupportedException> {
          rs.rowDeleted()
        }
      }
      "throw SQLFeatureNotSupportedException for update methods" {
        val schema = SchemaBuilder.record("wibble").fields().optionalString("foo").endRecord()
        val records = listOf(RecordRow(GenericRecordBuilder(schema).set("foo", "woo").build()))
        val rs = RowResultSet(null, schema, records)

        shouldThrow<SQLFeatureNotSupportedException> {
          rs.rowUpdated()
        }

        shouldThrow<SQLFeatureNotSupportedException> {
          rs.cancelRowUpdates()
        }

        shouldThrow<SQLFeatureNotSupportedException> {
          rs.updateRow()
        }
      }
    }
  }
}