package com.landoop.jdbc4

import com.landoop.rest.domain.InsertField
import com.landoop.rest.domain.PreparedInsertInfo
import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec
import org.apache.avro.SchemaBuilder
import java.sql.DriverManager
import java.sql.ParameterMetaData

class AvroSchemaParameterMetaDataTest : WordSpec() {

  init {
    LsqlDriver()

    val conn = DriverManager.getConnection("jdbc:lsql:kafka:https://master.lensesui.dev.landoop.com", "write", "write1")

    val fields = listOf(
        InsertField("mykey", emptyList(), true),
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
        meta.getParameterClassName(3) shouldBe "java.lang.String"
        meta.getParameterClassName(4) shouldBe "java.lang.Integer"
        meta.getParameterClassName(5) shouldBe "java.lang.Integer"
        meta.getParameterClassName(6) shouldBe "java.lang.Double"
        meta.getParameterClassName(7) shouldBe "java.lang.Double"
        meta.getParameterClassName(8) shouldBe "java.lang.Long"
        meta.getParameterClassName(9) shouldBe "java.lang.Long"
        meta.getParameterClassName(10) shouldBe "java.lang.Float"
        meta.getParameterClassName(11) shouldBe "java.lang.Float"
      }
      "return correct AVRO type for parameters" {
        meta.getParameterTypeName(1) shouldBe "STRING"
        meta.getParameterTypeName(2) shouldBe "STRING"
        meta.getParameterTypeName(3) shouldBe "STRING"
        meta.getParameterTypeName(4) shouldBe "INT"
        meta.getParameterTypeName(5) shouldBe "INT"
        meta.getParameterTypeName(6) shouldBe "DOUBLE"
        meta.getParameterTypeName(7) shouldBe "DOUBLE"
        meta.getParameterTypeName(8) shouldBe "LONG"
        meta.getParameterTypeName(9) shouldBe "LONG"
        meta.getParameterTypeName(10) shouldBe "FLOAT"
        meta.getParameterTypeName(11) shouldBe "FLOAT"
      }
      "return correct SQL type for parameters" {
        meta.getParameterType(2) shouldBe java.sql.Types.VARCHAR
        meta.getParameterType(4) shouldBe java.sql.Types.INTEGER
        meta.getParameterType(6) shouldBe 8
        meta.getParameterType(8) shouldBe -5
        meta.getParameterType(9) shouldBe -5
        meta.getParameterType(10) shouldBe 6
        meta.getParameterType(11) shouldBe 6
      }
      "detect number of fields from avro schema" {
        meta.parameterCount shouldBe 11
      }
      "detect signability" {
        meta.isSigned(1) shouldBe false
        meta.isSigned(2) shouldBe false
        meta.isSigned(3) shouldBe false
        meta.isSigned(4) shouldBe true
        meta.isSigned(5) shouldBe true
        meta.isSigned(6) shouldBe true
        meta.isSigned(7) shouldBe true
        meta.isSigned(8) shouldBe true
        meta.isSigned(9) shouldBe true
        meta.isSigned(10) shouldBe true
        meta.isSigned(11) shouldBe true
      }
      "all parameters should be mode in" {
        for (k in 1..10) {
          meta.getParameterMode(k) shouldBe ParameterMetaData.parameterModeIn
        }
      }
      "detect nullability" {
        meta.isNullable(1) shouldBe ParameterMetaData.parameterNoNulls
        meta.isNullable(2) shouldBe ParameterMetaData.parameterNullable
        meta.isNullable(3) shouldBe ParameterMetaData.parameterNoNulls
        meta.isNullable(4) shouldBe ParameterMetaData.parameterNullable
        meta.isNullable(5) shouldBe ParameterMetaData.parameterNoNulls
        meta.isNullable(6) shouldBe ParameterMetaData.parameterNullable
        meta.isNullable(7) shouldBe ParameterMetaData.parameterNoNulls
        meta.isNullable(8) shouldBe ParameterMetaData.parameterNullable
        meta.isNullable(9) shouldBe ParameterMetaData.parameterNoNulls
        meta.isNullable(10) shouldBe ParameterMetaData.parameterNullable
        meta.isNullable(11) shouldBe ParameterMetaData.parameterNoNulls
      }
      "detect field from a connection" {
        val stmt = conn.prepareStatement("INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values (?,?,?,?,?,?)")
        val meta = stmt.parameterMetaData
        meta.parameterCount shouldBe 6
        for (k in 1 until meta.parameterCount) {
          meta.getParameterClassName(1) shouldBe "java.lang.String"
          meta.getParameterType(1) shouldBe java.sql.Types.VARCHAR
          meta.getParameterTypeName(1) shouldBe "STRING"
          meta.getParameterMode(k) shouldBe ParameterMetaData.parameterModeIn
          meta.isNullable(k) shouldBe ParameterMetaData.parameterNoNulls
          meta.isSigned(k) shouldBe false
        }
        meta.getParameterClassName(6) shouldBe "java.lang.Boolean"
        meta.getParameterType(6) shouldBe java.sql.Types.BOOLEAN
        meta.getParameterTypeName(6) shouldBe "BOOLEAN"
        meta.getParameterMode(6) shouldBe ParameterMetaData.parameterModeIn
        meta.isNullable(6) shouldBe ParameterMetaData.parameterNoNulls
        meta.isSigned(6) shouldBe false
      }
      "detect fields for nested data" {
        val stmt = conn.prepareStatement("INSERT INTO users-mock-json (value.id, value.email, address.street, address.streetnumber, address.postalcode, address.state, personal.firstname, personal.lastname, personal.birthday, personal.title, lastlogingeo.lat, lastlogingeo.lon) values (?,?,?,?,?,?,?,?,?,?,?,?)")
        val meta = stmt.parameterMetaData
        meta.parameterCount shouldBe 12
      }
    }
  }
}