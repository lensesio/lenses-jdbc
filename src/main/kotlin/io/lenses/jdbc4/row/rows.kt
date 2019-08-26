package io.lenses.jdbc4.row

import org.apache.avro.generic.GenericData

/**
 * An implementation of [Row] that uses a static list of values
 * provided at construction time.
 */
class ListRow(private val array: List<Any?>) : ConvertingRow() {
  override fun getObject(index: Int): Any? = array[index - 1]
}

class RecordRow(val record: GenericData.Record) : ConvertingRow() {
  override fun getObject(index: Int): Any? {
    return record.get(index - 1)
  }
}