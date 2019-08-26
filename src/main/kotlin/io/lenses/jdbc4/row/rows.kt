package io.lenses.jdbc4.row

import org.apache.avro.generic.GenericData
import java.sql.SQLException

/**
 * An implementation of [Row] that uses a static list of values
 * provided at construction time.
 */
class ListRow(private val array: List<Any?>) : ConvertingRow() {
    override fun getObject(index: Int): Any? = try {
        array[index - 1]
    } catch (ex: IndexOutOfBoundsException) {
        throw SQLException("Invalid state for the record row.  Row column size is ${array.size} but ${index - 1} was requested.")
    }
}

class RecordRow(val record: GenericData.Record) : ConvertingRow() {
    override fun getObject(index: Int): Any? {
        return record.get(index - 1)
    }
}