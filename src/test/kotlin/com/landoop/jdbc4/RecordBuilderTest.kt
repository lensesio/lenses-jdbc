package com.landoop.jdbc4

import com.landoop.rest.domain.InsertField
import com.landoop.rest.domain.PreparedInsertInfo
import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldThrow
import io.kotlintest.specs.WordSpec
import org.apache.avro.SchemaBuilder
import java.sql.SQLException

class RecordBuilderTest : WordSpec() {
  init {

    val fields = listOf(
        InsertField("mykey", emptyList(), true),
        InsertField("a", emptyList(), false),
        InsertField("b", emptyList(), false)
    )

    val keySchema = SchemaBuilder.builder().stringType()

    val valueSchema = SchemaBuilder.record("wibble").fields()
        .requiredString("a")
        .requiredString("b")
        .endRecord()

    val info = PreparedInsertInfo("mytopic", fields, "avro", "avro", keySchema.toString(true), valueSchema.toString(true))

    "RecordBuilder.set" should {
      "throw an exception trying to set a parameter out of range" {
        shouldThrow<IndexOutOfBoundsException> {
          val builder = RecordBuilder(info)
          builder.put(0, "wibble")
        }
        shouldThrow<IndexOutOfBoundsException> {
          val builder = RecordBuilder(info)
          builder.put(123, "wibble")
        }
      }
    }
    "RecordBuilder.checkBounds" should {
      "throw no exceptions if the required values are set" {
        val builder = RecordBuilder(info)
        builder.put("mykey", "wibble")
        builder.put("a", "wibble")
        builder.put("b", "wibble")
        builder.checkRecord()
      }
      "throw an exception if a required key is not set" {
        shouldThrow<SQLException> {
          val builder = RecordBuilder(info)
          builder.put("a", "wibble")
          builder.put("b", "wibble")
          builder.checkRecord()
        }.message shouldBe "Key field must be specified"
      }
      "throw an exception if a parameter is not set" {
        shouldThrow<SQLException> {
          val builder = RecordBuilder(info)
          builder.put("mykey", "wibble")
          builder.put("b", "wibble")
          builder.checkRecord()
        }.message shouldBe "Variable a was not set; You must set all values before executing; if null is desired, explicitly set this using setNull(pos)"
      }
      "throw an exception if trying to set a value on a parent" {
        val fields = listOf(
            InsertField("mykey", emptyList(), true),
            InsertField("a", emptyList(), false),
            InsertField("b", emptyList(), false),
            InsertField("c", listOf("b"), false)
        )

        val b = SchemaBuilder.record("b").fields().requiredString("c").endRecord()
        val schema = SchemaBuilder.record("wibble").fields()
            .requiredString("a")
            .name("b").type(b).noDefault()
            .endRecord()

        val info = PreparedInsertInfo("mytopic", fields, "avro", "avro", keySchema.toString(true), schema.toString(true))

        shouldThrow<SQLException> {
          val builder = RecordBuilder(info)
          builder.put("mykey", "wibble")
          builder.put("a", "wibble")
          builder.put("b", "wibble")
          builder.put("b.c", "wibble")
          builder.checkRecord()
        }.message shouldBe "Can only insert values into leaf fields; b is a branch field"
      }
      "throw an exception if a nested parameter is not set" {

        val fields = listOf(
            InsertField("mykey", emptyList(), true),
            InsertField("a", emptyList(), false),
            InsertField("b", emptyList(), false),
            InsertField("c", listOf("b"), false)
        )

        val b = SchemaBuilder.record("b").fields().requiredString("c").endRecord()
        val schema = SchemaBuilder.record("wibble").fields()
            .requiredString("a")
            .name("b").type(b).noDefault()
            .endRecord()

        val info = PreparedInsertInfo("mytopic", fields, "avro", "avro", keySchema.toString(true), schema.toString(true))

        shouldThrow<SQLException> {
          val builder = RecordBuilder(info)
          builder.put("mykey", "wibble")
          builder.put("a", "wibble")
          builder.checkRecord()
        }.message shouldBe "Variable b.c was not set; You must set all values before executing; if null is desired, explicitly set this using setNull(pos)"
      }
    }
  }
}