package io.lenses.jdbc4.row

import java.sql.RowId

class LongRowId(val id: Long) : RowId {
  override fun getBytes(): ByteArray = id.toString().toByteArray()
}

/**
 * A [RowId] that wraps the offset field returned in Kafka.
 */
class OffsetRowId(val id: Long) : RowId {
  override fun getBytes(): ByteArray = id.toString().toByteArray()
}