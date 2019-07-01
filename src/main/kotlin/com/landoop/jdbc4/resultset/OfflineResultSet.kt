package com.landoop.jdbc4.resultset

import java.sql.ResultSet
import java.sql.SQLException

/**
 * Partial implementation of [ResultSet] for such result sets that are
 * offline. Offline means resultsets that do not maintain state with
 * the server, such as HTTP based result sets.
 */
interface OfflineResultSet : ResultSet {

  override fun isClosed(): Boolean = true

  // todo should feed back to the socket to cancel any further data
  override fun close() {}

  // streaming resultsets can only go forwards
  override fun getType(): Int = ResultSet.TYPE_FORWARD_ONLY

  override fun getFetchDirection(): Int = ResultSet.FETCH_FORWARD
  override fun setFetchDirection(direction: Int): Unit = when (direction) {
    ResultSet.FETCH_FORWARD -> Unit
    else -> throw SQLException("Unsupported fetch direction $direction")
  }

  override fun getFetchSize(): Int = -1
  override fun setFetchSize(rows: Int) {} // no op since this resultset is streaming
}

