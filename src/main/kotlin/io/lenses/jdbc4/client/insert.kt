package io.lenses.jdbc4.client

import io.lenses.jdbc4.client.domain.InsertField
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream

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
