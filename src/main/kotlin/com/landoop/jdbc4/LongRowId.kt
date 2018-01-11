package com.landoop.jdbc4

import java.sql.RowId

class LongRowId(val id: Long) : RowId {
  override fun getBytes(): ByteArray = id.toString().toByteArray()
}