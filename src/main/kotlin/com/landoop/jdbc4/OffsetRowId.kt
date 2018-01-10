package com.landoop.jdbc4

import java.sql.RowId

class OffsetRowId(val id: String) : RowId {
  override fun getBytes(): ByteArray = id.toByteArray()
}