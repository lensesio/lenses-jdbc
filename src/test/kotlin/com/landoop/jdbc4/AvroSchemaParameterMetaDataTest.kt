package com.landoop.jdbc4

import com.landoop.rest.domain.InsertField
import com.landoop.rest.domain.PreparedInsertInfo
import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec
import org.apache.avro.SchemaBuilder
import java.sql.ParameterMetaData

class AvroSchemaParameterMetaDataTest : WordSpec() {

  init {

    val fields = listOf(
        InsertField("optstring", emptyList(), false),
        InsertField("reqstring", emptyList(), false),
        InsertField("optint", emptyList(), false),
        InsertField("reqint", emptyList(), false),
        InsertField("optdouble", emptyList(), false),
        InsertField("reqdouble", emptyList(), false),
        InsertField("optlong", emptyList(), false),
        InsertField("reqlong", emptyList(), false),
        InsertField("optfloat", emptyList(), false),
        InsertField("reqfloat", emptyList(), false)
    )

    val valueSchema = SchemaBuilder.record("wibble").fields()
        .optionalString("optstring")
        .requiredString("reqstring")
        .optionalInt("optint")
        .requiredInt("reqint")
        .optionalDouble("optdouble")
        .requiredDouble("reqdouble")
        .optionalLong("optlong")
        .requiredLong("reqlong")
        .optionalFloat("optfloat")
        .requiredFloat("reqfloat")
        .endRecord()

    val info = PreparedInsertInfo("anytopic", fields, "avro", "avro", null, valueSchema.toString(true))
    val meta = AvroSchemaParameterMetaData(info)

    "AvroSchemaParameterMetaData" should {
      "return correct JVM type for parameters" {
        meta.getParameterClassName(1) shouldBe "java.lang.String"
        meta.getParameterClassName(2) shouldBe "java.lang.String"
        meta.getParameterClassName(3) shouldBe "java.lang.Integer"
        meta.getParameterClassName(4) shouldBe "java.lang.Integer"
        meta.getParameterClassName(5) shouldBe "java.lang.Double"
        meta.getParameterClassName(6) shouldBe "java.lang.Double"
        meta.getParameterClassName(7) shouldBe "java.lang.Long"
        meta.getParameterClassName(8) shouldBe "java.lang.Long"
        meta.getParameterClassName(9) shouldBe "java.lang.Float"
        meta.getParameterClassName(10) shouldBe "java.lang.Float"
      }
      "return correct AVRO type for parameters" {
        meta.getParameterTypeName(1) shouldBe "STRING"
        meta.getParameterTypeName(3) shouldBe "INT"
        meta.getParameterTypeName(5) shouldBe "DOUBLE"
        meta.getParameterTypeName(7) shouldBe "LONG"
        meta.getParameterTypeName(9) shouldBe "FLOAT"
      }
      "return correct SQL type for parameters" {
        meta.getParameterType(1) shouldBe 12
        meta.getParameterType(3) shouldBe 4
        meta.getParameterType(5) shouldBe 8
        meta.getParameterType(7) shouldBe -5
        meta.getParameterType(9) shouldBe 6
      }
      "detect number of fields from avro schema" {
        meta.parameterCount shouldBe 10
      }
      "detect signability" {
        meta.isSigned(1) shouldBe false
        meta.isSigned(2) shouldBe false
        meta.isSigned(3) shouldBe true
        meta.isSigned(4) shouldBe true
        meta.isSigned(5) shouldBe true
        meta.isSigned(6) shouldBe true
        meta.isSigned(7) shouldBe true
        meta.isSigned(8) shouldBe true
        meta.isSigned(9) shouldBe true
        meta.isSigned(10) shouldBe true
      }
      "all parameters should be mode in" {
        for (k in 1..10) {
          meta.getParameterMode(k) shouldBe ParameterMetaData.parameterModeIn
        }
      }
      "detect nullability" {
        meta.isNullable(1) shouldBe ParameterMetaData.parameterNullable
        meta.isNullable(2) shouldBe ParameterMetaData.parameterNoNulls
        meta.isNullable(3) shouldBe ParameterMetaData.parameterNullable
        meta.isNullable(4) shouldBe ParameterMetaData.parameterNoNulls
        meta.isNullable(5) shouldBe ParameterMetaData.parameterNullable
        meta.isNullable(6) shouldBe ParameterMetaData.parameterNoNulls
        meta.isNullable(7) shouldBe ParameterMetaData.parameterNullable
        meta.isNullable(8) shouldBe ParameterMetaData.parameterNoNulls
        meta.isNullable(9) shouldBe ParameterMetaData.parameterNullable
        meta.isNullable(10) shouldBe ParameterMetaData.parameterNoNulls
      }
    }
  }
}