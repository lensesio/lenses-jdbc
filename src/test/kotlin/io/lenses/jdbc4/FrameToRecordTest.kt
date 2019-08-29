package io.lenses.jdbc4

import arrow.core.getOrHandle
import io.kotlintest.fail
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.client.frameToRecord
import org.apache.avro.SchemaBuilder
import java.sql.SQLException

class FrameToRecordTest : FunSpec() {
  init {
    test("frame to record for flat schema with all values") {
      val schema = SchemaBuilder.builder().record("foo")
          .fields()
          .optionalString("a")
          .requiredBoolean("b")
          .optionalInt("c")
          .endRecord()
      val json = """{ "data" : { "value": { "a":"hello", "b":true, "c": 123 } } }"""
      val row = frameToRecord(json, schema).getOrHandle { fail(it.toString()) }
      row.getObject(1) shouldBe "hello"
      row.getObject(2) shouldBe true
      row.getObject(3) shouldBe 123

      shouldThrow<SQLException> { row.getObject(4) }
    }

    test("frame to record for flat schema with missing values") {
      val schema = SchemaBuilder.builder().record("foo")
          .fields()
          .optionalString("a")
          .requiredBoolean("b")
          .optionalInt("c")
          .endRecord()
      val json = """{ "data" : { "value": { "a":"hello", "c": 123 } } }"""
      val row = frameToRecord(json, schema).getOrHandle { fail(it.toString()) }
      row.getObject(1) shouldBe "hello"
      row.getObject(2) shouldBe null
      row.getObject(3) shouldBe 123

      shouldThrow<SQLException> { row.getObject(4) }
    }

    test("frame to record for nested schema") {

      val b = SchemaBuilder.builder().record("b")
          .fields()
          .optionalString("x")
          .requiredLong("y")
          .endRecord()

      val schema = SchemaBuilder.builder().record("foo")
          .fields()
          .optionalString("a")
          .name("b").type(b).noDefault()
          .optionalInt("c")
          .endRecord()

      val json = """{ "data" : { "value": {"a": "hello", "b": { "x": "world", "y": 999 }, "c": 123} } }"""
      val row = frameToRecord(json, schema).getOrHandle { fail(it.toString()) }
      row.getObject(1) shouldBe "hello"
      row.getObject(2) shouldBe "world"
      row.getObject(3) shouldBe 999
      row.getObject(4) shouldBe 123

      shouldThrow<SQLException> { row.getObject(5) }
    }

    test("frame to record for nested schema with missing values") {

      val b = SchemaBuilder.builder().record("b")
          .fields()
          .optionalString("x")
          .requiredLong("y")
          .endRecord()

      val schema = SchemaBuilder.builder().record("foo")
          .fields()
          .optionalString("a")
          .name("b").type(b).noDefault()
          .optionalInt("c")
          .endRecord()

      val json = """{ "data" : { "value": {"a": "hello", "b": { "x": "world" }} } }"""
      val row = frameToRecord(json, schema).getOrHandle { fail(it.toString()) }
      row.getObject(1) shouldBe "hello"
      row.getObject(2) shouldBe "world"
      row.getObject(3) shouldBe null
      row.getObject(4) shouldBe null

      shouldThrow<SQLException> { row.getObject(5) }
    }
  }
}