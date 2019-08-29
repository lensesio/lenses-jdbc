package io.lenses.jdbc4

import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.avro.SchemaBuilder

class NormalizeRecordTest : FunSpec() {
  init {

    test("normalizing flat schema with all values") {
      val schema = SchemaBuilder.builder().record("foo")
          .fields()
          .optionalString("a")
          .requiredBoolean("b")
          .optionalInt("c")
          .endRecord()
      val node = JacksonSupport.mapper.readTree("""{"a":"hello", "b":true, "c": 123}""")
      normalizeRecord(schema, node) shouldBe listOf("a" to "hello", "b" to true, "c" to 123)
    }

    test("normalizing flat schema with all values unordered") {
      val schema = SchemaBuilder.builder().record("foo")
          .fields()
          .optionalString("a")
          .requiredBoolean("b")
          .optionalInt("c")
          .endRecord()
      val node = JacksonSupport.mapper.readTree("""{"a":"hello", "c":123, "b": true}""")
      normalizeRecord(schema, node) shouldBe listOf("a" to "hello", "b" to true, "c" to 123)
    }

    test("normalizing flat schema with missing values") {
      val schema = SchemaBuilder.builder().record("foo")
          .fields()
          .optionalString("a")
          .requiredBoolean("b")
          .optionalInt("c")
          .endRecord()
      val node = JacksonSupport.mapper.readTree("""{"a":"hello", "c":123}""")
      normalizeRecord(schema, node) shouldBe listOf("a" to "hello", "b" to null, "c" to 123)
    }

    test("normalizing flat schema with missing values unordered") {
      val schema = SchemaBuilder.builder().record("foo")
          .fields()
          .optionalString("a")
          .requiredBoolean("b")
          .optionalInt("c")
          .endRecord()
      val node = JacksonSupport.mapper.readTree("""{"c":123, "a":"hello"}""")
      normalizeRecord(schema, node) shouldBe listOf("a" to "hello", "b" to null, "c" to 123)
    }

    test("normalizing nested schema with all values") {

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
      val node = JacksonSupport.mapper.readTree("""{"a": "hello", "b": { "x": "world", "y": 999 }, "c": 123}""")
      normalizeRecord(schema, node) shouldBe listOf("a" to "hello", "b.x" to "world", "b.y" to 999, "c" to 123)
    }

    test("normalizing nested schema with missing values") {

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
      val node = JacksonSupport.mapper.readTree("""{"a": "hello", "b": { "x": "world" }}""")
      normalizeRecord(schema, node) shouldBe listOf("a" to "hello", "b.x" to "world", "b.y" to null, "c" to null)
    }
  }
}