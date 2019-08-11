package com.landoop.jdbc4.resultset

import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException

/**
 * Partial implementation of [ResultSet] for such result sets that are
 * pull-based forward moving only. This means that the cursor cannot be manipulated by
 * the user of this resultset, other than invoking next to move along the stream.
 */
interface PullForwardOnlyResultSet : ResultSet {

  override fun absolute(row: Int): Boolean = throw SQLFeatureNotSupportedException()
  override fun relative(rows: Int): Boolean = throw SQLFeatureNotSupportedException()
  override fun previous() = throw SQLFeatureNotSupportedException()
  override fun beforeFirst() = throw SQLFeatureNotSupportedException()
  override fun afterLast() = throw SQLFeatureNotSupportedException()
  override fun first() = throw SQLFeatureNotSupportedException()
  override fun last() = throw SQLFeatureNotSupportedException()

  override fun getFetchSize(): Int = -1
  override fun setFetchSize(rows: Int) {} // no op since this resultset is streaming

  override fun getFetchDirection(): Int = ResultSet.FETCH_FORWARD
  override fun setFetchDirection(direction: Int): Unit = when (direction) {
    ResultSet.FETCH_FORWARD -> Unit
    else -> throw SQLException("Unsupported fetch direction $direction")
  }

  // streaming result sets can only go forwards
  override fun getType(): Int = ResultSet.TYPE_FORWARD_ONLY

  override fun isLast(): Boolean = false
  override fun isFirst(): Boolean = row == 1
  override fun isBeforeFirst(): Boolean = row < 1
  override fun isAfterLast(): Boolean = false
}