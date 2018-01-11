package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder

class RecordResultSetTest : WordSpec() {
  init {
    "LsqlResultSet" should {
      "use next to iterate records" {

      }
      "support by label lookup" {

      }
      "getString of null should return null" {

        val schema = SchemaBuilder.record("wibble")
            .fields()
            .optionalString("foo")
            .endRecord()

        val records = listOf(RecordRow(1, GenericRecordBuilder(schema).set("foo", null).build()))

        val rs = RecordResultSet(null, schema, records)
        rs.next()
        rs.getString("foo") shouldBe null
      }
      "support absolute positive position" {

      }
      "support absolute negative position" {

      }
      "support relative positioning" {

      }
      "support isFirst()" {

      }
      "support isLast()" {

      }
      "support isAfterLast()" {

      }
      "support isBeforeFirst()" {
      }
      "next should honour fetch direction" {

      }
      "previous shuld honour fetch direction" {

      }
      "track last value to support wasNull" {

        val schema = SchemaBuilder.record("wibble")
            .fields()
            .optionalString("foo")
            .endRecord()
        val records = listOf(
            RecordRow(0, GenericRecordBuilder(schema).set("foo", "woo").build()),
            RecordRow(1, GenericRecordBuilder(schema).set("foo", null).build())
        )
        val rs = RecordResultSet(null, schema, records)
        rs.next()
        rs.getString("foo") shouldBe "woo"
        rs.wasNull() shouldBe false

        rs.next()
        rs.getString("foo") shouldBe null
        rs.wasNull() shouldBe true
      }
    }
  }
}