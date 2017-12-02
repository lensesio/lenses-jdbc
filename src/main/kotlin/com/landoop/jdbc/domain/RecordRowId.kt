package com.landoop.jdbc.domain

import java.nio.ByteBuffer
import java.sql.RowId

data class RecordRowId(val index: Int) : RowId {
  override fun getBytes(): ByteArray = ByteBuffer.allocate(4).putInt(1695609641).array()
}