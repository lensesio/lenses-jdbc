package com.landoop.jdbc4

import com.landoop.jdbc4.client.domain.InsertField
import com.landoop.jdbc4.client.domain.PreparedInsertInfo
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
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

    val nestedFields = listOf(
        InsertField("mykey", emptyList(), true),
        InsertField("a", emptyList(), false),
        InsertField("c", listOf("b"), false)
    )

    val keySchema = SchemaBuilder.builder().stringType()

    val valueSchema = SchemaBuilder.record("wibble").fields()
        .requiredString("a")
        .requiredString("b")
        .endRecord()

    val b = SchemaBuilder.record("b").fields().requiredString("c").endRecord()

    val nestedSchema = SchemaBuilder.record("wibble").fields()
        .requiredString("a")
        .name("b").type(b).noDefault()
        .endRecord()

    val info = PreparedInsertInfo("mytopic", fields, "avro", "avro", keySchema.toString(true), valueSchema.toString(true))
    val nestedInfo = PreparedInsertInfo("mytopic", nestedFields, "avro", "avro", keySchema.toString(true), nestedSchema.toString(true))

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
        builder.put(1, "wibble")
        builder.put(2, "wibble")
        builder.put(3, "wibble")
        builder.checkRecord()
      }
      "throw no exceptions if the required values are set on nested values" {
        val builder = RecordBuilder(nestedInfo)
        builder.put(1, "wibble")
        builder.put(2, "wibble")
        builder.put(3, "wibble")
        builder.checkRecord()
      }
      "throw an exception if a required key is not set" {
        shouldThrow<SQLException> {
          val builder = RecordBuilder(info)
          builder.put(2, "wibble")
          builder.put(3, "wibble")
          builder.checkRecord()
        }.message shouldBe "Variable mykey was not set; You must set all values before executing; if null is desired, explicitly set this using setNull(pos)"
      }
      "throw an exception if a parameter is not set" {
        shouldThrow<SQLException> {
          val builder = RecordBuilder(info)
          builder.put(1, "wibble")
          builder.put(3, "wibble")
          builder.checkRecord()
        }.message shouldBe "Variable a was not set; You must set all values before executing; if null is desired, explicitly set this using setNull(pos)"
      }
      "throw an exception if a nested parameter is not set" {
        shouldThrow<SQLException> {
          val builder = RecordBuilder(nestedInfo)
          builder.put(1, "wibble")
          builder.put(2, "wibble")
          builder.checkRecord()
        }.message shouldBe "Variable b.c was not set; You must set all values before executing; if null is desired, explicitly set this using setNull(pos)"
      }
      "build flat json structure" {
        val builder = RecordBuilder(info)
        builder.put(1, "wibble")
        builder.put(2, "woo")
        builder.put(3, "foo")
        val (key, node) = builder.build()
        key.asText() shouldBe "wibble"
        JacksonSupport.mapper.writeValueAsString(node) shouldBe """{"a":"woo","b":"foo"}"""
      }
      "build nested json structure" {
        val builder = RecordBuilder(nestedInfo)
        builder.put(1, "wibble")
        builder.put(2, "woo")
        builder.put(3, "foo")
        val (key, node) = builder.build()
        key.asText() shouldBe "wibble"
        JacksonSupport.mapper.writeValueAsString(node) shouldBe """{"a":"woo","b":{"c":"foo"}}"""
      }
    }
  }
}