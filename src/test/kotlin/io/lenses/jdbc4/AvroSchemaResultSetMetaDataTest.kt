//package com.landoop.jdbc4
//
//import com.landoop.jdbc4.resultset.AvroSchemaResultSetMetaData
//import io.kotlintest.shouldBe
//import io.kotlintest.specs.WordSpec
//import org.apache.avro.LogicalTypes
//import org.apache.avro.Schema
//import org.apache.avro.SchemaBuilder
//import org.apache.avro.generic.GenericData
//import java.math.BigDecimal
//import java.nio.ByteBuffer
//import java.sql.ResultSetMetaData
//import java.util.*
//
//class AvroSchemaResultSetMetaDataTest : WordSpec() {
//  init {
//    "LsqlResultSetMetaDataTest" should {
//      "find column for a label" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").requiredString("b").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        record.put("b", "wobble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.indexForLabel("a") shouldBe 1
//        meta.indexForLabel("b") shouldBe 2
//      }
//      "return signed for numericals" {
//        val schema = SchemaBuilder.record("foo").fields().requiredLong("a").requiredInt("b").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", 1L)
//        record.put("b", 2)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.isSigned(1) shouldBe true
//        meta.isSigned(2) shouldBe true
//      }
//      "return not signed for non numericals" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").requiredBoolean("b").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        record.put("b", true)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.isSigned(1) shouldBe false
//        meta.isSigned(2) shouldBe false
//      }
//      "return read only" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.isReadOnly(1) shouldBe true
//      }
//      "is not writable" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.isWritable(1) shouldBe false
//      }
//      "is not auto increment" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.isAutoIncrement(1) shouldBe false
//      }
//      "is case sensitive" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.isCaseSensitive(1) shouldBe true
//      }
//      "is searchable" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.isSearchable(1) shouldBe true
//      }
//      "is not currency" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.isCurrency(1) shouldBe false
//      }
//      "is not definitely writable" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.isDefinitelyWritable(1) shouldBe false
//      }
//      "return correct name for index" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnName(1) shouldBe "a"
//      }
//      "return correct column count" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").optionalString("b").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        record.put("b", null)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.columnCount shouldBe 2
//      }
//      "specify nullability" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").optionalString("b").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        record.put("b", null)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.isNullable(1) shouldBe ResultSetMetaData.columnNullableUnknown
//        meta.isNullable(2) shouldBe ResultSetMetaData.columnNullable
//      }
//      "return correct java.sql type for numericals" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredLong("a")
//            .requiredInt("b")
//            .requiredFloat("c")
//            .requiredDouble("d")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", 123L)
//        record.put("b", 123)
//        record.put("c", 1.4F)
//        record.put("d", 123.34)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnType(1) shouldBe java.sql.Types.BIGINT
//        meta.getColumnType(2) shouldBe java.sql.Types.INTEGER
//        meta.getColumnType(3) shouldBe java.sql.Types.FLOAT
//        meta.getColumnType(4) shouldBe java.sql.Types.DOUBLE
//      }
//      "return correct java sql type for non numericals" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredString("a")
//            .requiredBoolean("b")
//            .requiredBytes("c")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        record.put("b", true)
//        record.put("c", null)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnType(1) shouldBe java.sql.Types.VARCHAR
//        meta.getColumnType(2) shouldBe java.sql.Types.BOOLEAN
//        meta.getColumnType(3) shouldBe java.sql.Types.BINARY
//      }
//      "return correct java sql type for logical decimal" {
//        val decimal = SchemaBuilder.builder().bytesType()
//        LogicalTypes.decimal(10, 4).addToSchema(decimal)
//
//        val schema = SchemaBuilder.record("foo").fields()
//            .name("a").type(decimal).noDefault()
//            .endRecord()
//
//        val record = GenericData.Record(schema)
//        record.put("a", ByteBuffer.wrap(BigDecimal(123).unscaledValue().toByteArray()))
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnType(1) shouldBe java.sql.Types.DECIMAL
//      }
//      "return correct java sql type for optionals/unions" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .optionalString("a")
//            .optionalBoolean("b")
//            .optionalBytes("c")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        record.put("b", true)
//        record.put("c", null)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnType(1) shouldBe java.sql.Types.VARCHAR
//        meta.getColumnType(2) shouldBe java.sql.Types.BOOLEAN
//        meta.getColumnType(3) shouldBe java.sql.Types.BINARY
//      }
//      "return correct java sql type for logical date" {
//        val date = SchemaBuilder.builder().intType()
//        LogicalTypes.date().addToSchema(date)
//
//        val schema = SchemaBuilder.record("foo").fields()
//            .name("a").type(date).noDefault()
//            .endRecord()
//
//        val record = GenericData.Record(schema)
//        record.put("a", Date())
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnType(1) shouldBe java.sql.Types.DATE
//      }
//      "return correct java sql type for logical time" {
//        val time = SchemaBuilder.builder().intType()
//        LogicalTypes.timeMillis().addToSchema(time)
//
//        val schema = SchemaBuilder.record("foo").fields()
//            .name("a").type(time).noDefault()
//            .endRecord()
//
//        val record = GenericData.Record(schema)
//        record.put("a", java.sql.Time(123123123L))
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnType(1) shouldBe java.sql.Types.TIME
//      }
//      "return correct java sql type for logical timestamp" {
//        val decimal = SchemaBuilder.builder().longType()
//        LogicalTypes.timestampMillis().addToSchema(decimal)
//
//        val schema = SchemaBuilder.record("foo").fields()
//            .name("a").type(decimal).noDefault()
//            .endRecord()
//
//        val record = GenericData.Record(schema)
//        record.put("a", ByteBuffer.wrap(BigDecimal(123).unscaledValue().toByteArray()))
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnType(1) shouldBe java.sql.Types.TIMESTAMP
//      }
//      "return correct java sql type for logical UUID" {
//
//        val decimal = SchemaBuilder.builder().bytesType()
//        LogicalTypes.uuid().addToSchema(decimal)
//
//        val schema = SchemaBuilder.record("foo").fields()
//            .name("a").type(decimal).noDefault()
//            .endRecord()
//
//        val record = GenericData.Record(schema)
//        record.put("a", ByteBuffer.wrap(BigDecimal(123).unscaledValue().toByteArray()))
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnType(1) shouldBe java.sql.Types.VARCHAR
//      }
//      "return the JVM classname for a String" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredString("a")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnClassName(1) shouldBe String::class.java.canonicalName
//      }
//      "return the JVM classname for a boolean" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredBoolean("a")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", true)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnClassName(1) shouldBe "java.lang.Boolean"
//      }
//      "return the JVM classname for a int" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredInt("a")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", 123)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnClassName(1) shouldBe "java.lang.Integer"
//      }
//      "return the JVM classname for a long" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredLong("a")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", 123L)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnClassName(1) shouldBe "java.lang.Long"
//      }
//      "return the JVM classname for a float" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredFloat("a")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", 12.34F)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnClassName(1) shouldBe "java.lang.Float"
//      }
//      "return the JVM classname for a double" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredDouble("a")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", 12.34)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnClassName(1) shouldBe "java.lang.Double"
//      }
//      "return the JVM classname for a bytes type" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredBytes("a")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", ByteBuffer.wrap(byteArrayOf(1, 2, 3)))
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnClassName(1) shouldBe ByteArray(1).javaClass.canonicalName
//      }
//      "return the JVM classname for an enum type" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .name("a").type(SchemaBuilder.enumeration("foo").symbols("a", "b")).noDefault()
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "a")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnClassName(1) shouldBe String::class.java.canonicalName
//      }
//      "return the avro name for a string" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredString("a")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnTypeName(1) shouldBe Schema.Type.STRING.name
//      }
//      "return the avro name for a double" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredDouble("a")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", 12.34)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnTypeName(1) shouldBe Schema.Type.DOUBLE.name
//      }
//      "return the avro name for a float" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredFloat("a")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", 12.34F)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnTypeName(1) shouldBe Schema.Type.FLOAT.name
//      }
//      "return the avro name for a boolean" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredBoolean("a")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", true)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnTypeName(1) shouldBe Schema.Type.BOOLEAN.name
//      }
//      "return the avro name for a long" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredLong("a")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", 123L)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnTypeName(1) shouldBe Schema.Type.LONG.name
//      }
//      "return the avro name for an int" {
//        val schema = SchemaBuilder.record("foo").fields()
//            .requiredInt("a")
//            .endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", 123)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getColumnTypeName(1) shouldBe Schema.Type.INT.name
//      }
//      "return the correct field for a given label" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").requiredBoolean("b").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        record.put("b", true)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.fieldForLabel("a") shouldBe schema.fields[0]
//        meta.fieldForLabel("b") shouldBe schema.fields[1]
//      }
//      "return the correct index for a given label" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").requiredBoolean("b").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        record.put("b", true)
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.indexForLabel("a") shouldBe 1
//        meta.indexForLabel("b") shouldBe 2
//      }
//      "use empty string for catalog name" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getCatalogName(1) shouldBe ""
//      }
//      "use empty string for schema name" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getSchemaName(1) shouldBe ""
//      }
//      "use the schema name as the table name" {
//        val schema = SchemaBuilder.record("foo").fields().requiredString("a").endRecord()
//        val record = GenericData.Record(schema)
//        record.put("a", "wibble")
//        val meta = AvroSchemaResultSetMetaData(schema,
//            RowResultSet.fromRecords(schema, listOf(record)))
//        meta.getTableName(1) shouldBe "foo"
//      }
//    }
//  }
//}