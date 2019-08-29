package io.lenses.jdbc4.row

import org.apache.avro.generic.GenericData
import java.sql.SQLException

/**
 * An implementation of [Row] that uses a static list of values
 * provided at construction time.
 */
class ListRow(private val values: List<Any?>) : ConvertingRow() {
  override fun getObject(index: Int): Any? = try {
    values[index - 1]
  } catch (ex: IndexOutOfBoundsException) {
    throw SQLException("Column index out of bounds $index")
  }
}

class PairRow(private val values: List<Pair<String, Any?>>) : ConvertingRow() {
  override fun getObject(index: Int): Any? = try {
    values[index - 1].second
  } catch (ex: IndexOutOfBoundsException) {
    throw SQLException("Column index out of bounds $index")
  }
}

class RecordRow(val record: GenericData.Record) : ConvertingRow() {
  override fun getObject(index: Int): Any? {
    return record.get(index - 1)
  }
}