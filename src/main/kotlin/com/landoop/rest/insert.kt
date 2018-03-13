package com.landoop.rest

import com.landoop.rest.domain.InsertField
import com.landoop.rest.domain.InsertRecord
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import java.sql.SQLException

fun rowToInsert(schema: Schema, fields: List<InsertField>, row: List<Any?>): InsertRecord {

  val keypos = fields.indexOfFirst { it.isKey }
  if (keypos == -1)
    throw SQLException("No key field defined")
  val key = row[keypos].toString()

  val value = recordToJson(rowToValueRecord(schema, fields, row))
  return InsertRecord(key, value)
}

fun rowToValueRecord(schema: Schema, fields: List<InsertField>, row: List<Any?>): GenericRecord {
  val record = GenericData.Record(schema)
  schema.fields.forEach { field ->
    val rowIndex = fields.indexOfFirst { it.name == field.name() }
    val value = when (rowIndex) {
    // if -1 then the schema field did not exist in the insert statement, so we pad with null
      -1 -> null
    // otherwise we know the offset into the row
      else -> row[rowIndex]
    }
    record.put(field.name(), value)
  }
  return record
}

fun recordToJson(record: GenericRecord): String {
  val writer = GenericDatumWriter<GenericRecord>(record.schema)
  val baos = ByteArrayOutputStream()
  val jsonEncoder = EncoderFactory.get().jsonEncoder(record.schema, baos)
  writer.write(record, jsonEncoder)
  jsonEncoder.flush()
  return String(baos.toByteArray())
}
