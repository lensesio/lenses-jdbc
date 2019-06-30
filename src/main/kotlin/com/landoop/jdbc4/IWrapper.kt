package com.landoop.jdbc4

import java.sql.SQLException

interface IWrapper {
  fun _isWrapperFor(iface: Class<*>?): Boolean = iface?.isInstance(iface) ?: false
  fun <T : Any?> _unwrap(iface: Class<T>): T {
    try {
      return iface.cast(this)
    } catch (cce: ClassCastException) {
      throw SQLException("Unable to unwrap instance as $iface")
    }
  }
}
