package com.landoop.jdbc4.resultset

import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.sql.Blob
import java.sql.Clob
import java.sql.Date
import java.sql.NClob
import java.sql.Ref
import java.sql.ResultSet
import java.sql.RowId
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLXML
import java.sql.Time
import java.sql.Timestamp

/**
 * Partial implementation of [ResultSet] for immutability.
 * Any functions that update the underlying data source throw an exception.
 * Any functions that enquire as to the availability of such functions return
 * the read only values.
 */
interface ImmutableResultSet : ResultSet {

  // we do not support updates, so this method is irrelevant
  // CLOSE is the nearest match
  override fun getHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT

  /**
   * Returns the constant indicating the concurrency mode for a
   * ResultSet object that may NOT be updated.
   */
  override fun getConcurrency(): Int = ResultSet.CONCUR_READ_ONLY

  override fun refreshRow() {}
  override fun rowInserted(): Boolean = throw SQLFeatureNotSupportedException()
  override fun rowDeleted(): Boolean = throw SQLFeatureNotSupportedException()
  override fun moveToInsertRow() = throw SQLFeatureNotSupportedException()
  override fun insertRow() = throw SQLFeatureNotSupportedException()
  override fun rowUpdated(): Boolean = throw SQLFeatureNotSupportedException()
  override fun moveToCurrentRow() {}
  override fun cancelRowUpdates() = throw SQLFeatureNotSupportedException()

  override fun updateShort(index: Int, x: Short) = throw SQLFeatureNotSupportedException()
  override fun updateShort(label: String?, x: Short) = throw SQLFeatureNotSupportedException()
  override fun updateLong(index: Int, x: Long) = throw SQLFeatureNotSupportedException()
  override fun updateLong(label: String?, x: Long) = throw SQLFeatureNotSupportedException()
  override fun updateClob(index: Int, x: Clob?) = throw SQLFeatureNotSupportedException()
  override fun updateClob(label: String?, x: Clob?) = throw SQLFeatureNotSupportedException()
  override fun updateClob(index: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateClob(label: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateClob(index: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateClob(label: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateTimestamp(index: Int, x: Timestamp?) = throw SQLFeatureNotSupportedException()
  override fun updateTimestamp(label: String?, x: Timestamp?) = throw SQLFeatureNotSupportedException()
  override fun updateNCharacterStream(index: Int, x: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateNCharacterStream(label: String?,
                                      reader: Reader?,
                                      length: Long) = throw SQLFeatureNotSupportedException()

  override fun updateNCharacterStream(index: Int, x: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateNCharacterStream(label: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateInt(index: Int, x: Int) = throw SQLFeatureNotSupportedException()
  override fun updateInt(label: String?, x: Int) = throw SQLFeatureNotSupportedException()
  override fun updateNString(index: Int, nString: String?) = throw SQLFeatureNotSupportedException()
  override fun updateNString(label: String?, nString: String?) = throw SQLFeatureNotSupportedException()
  override fun updateBinaryStream(index: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
  override fun updateBinaryStream(label: String?,
                                  x: InputStream?,
                                  length: Int) = throw SQLFeatureNotSupportedException()

  override fun updateBinaryStream(index: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateBinaryStream(label: String?,
                                  x: InputStream?,
                                  length: Long) = throw SQLFeatureNotSupportedException()

  override fun updateBinaryStream(index: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun updateBinaryStream(label: String?, x: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun updateNull(index: Int) = throw SQLFeatureNotSupportedException()
  override fun updateNull(label: String?) = throw SQLFeatureNotSupportedException()
  override fun updateCharacterStream(index: Int, x: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateCharacterStream(label: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateCharacterStream(index: Int, x: Reader?, length: Int) = throw SQLFeatureNotSupportedException()
  override fun updateCharacterStream(label: String?,
                                     reader: Reader?,
                                     length: Int) = throw SQLFeatureNotSupportedException()

  override fun updateCharacterStream(index: Int, x: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateCharacterStream(label: String?,
                                     reader: Reader?,
                                     length: Long) = throw SQLFeatureNotSupportedException()

  override fun updateBoolean(index: Int, x: Boolean) = throw SQLFeatureNotSupportedException()
  override fun updateBoolean(label: String?, x: Boolean) = throw SQLFeatureNotSupportedException()
  override fun updateBigDecimal(index: Int, x: BigDecimal?) = throw SQLFeatureNotSupportedException()
  override fun updateBigDecimal(label: String?, x: BigDecimal?) = throw SQLFeatureNotSupportedException()
  override fun updateString(index: Int, x: String?) = throw SQLFeatureNotSupportedException()
  override fun updateString(label: String?, x: String?) = throw SQLFeatureNotSupportedException()
  override fun updateTime(index: Int, x: Time?) = throw SQLFeatureNotSupportedException()
  override fun updateTime(label: String?, x: Time?) = throw SQLFeatureNotSupportedException()
  override fun updateNClob(index: Int, nClob: NClob?) = throw SQLFeatureNotSupportedException()
  override fun updateNClob(label: String?, nClob: NClob?) = throw SQLFeatureNotSupportedException()
  override fun updateNClob(index: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateNClob(label: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateNClob(index: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateNClob(label: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateRef(index: Int, x: Ref?) = throw SQLFeatureNotSupportedException()
  override fun updateRef(label: String?, x: Ref?) = throw SQLFeatureNotSupportedException()
  override fun updateObject(index: Int, x: Any?, scaleOrLength: Int) = throw SQLFeatureNotSupportedException()
  override fun updateObject(index: Int, x: Any?) = throw SQLFeatureNotSupportedException()
  override fun updateObject(label: String?, x: Any?, scaleOrLength: Int) = throw SQLFeatureNotSupportedException()
  override fun updateObject(label: String?, x: Any?) = throw SQLFeatureNotSupportedException()
  override fun updateAsciiStream(index: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
  override fun updateAsciiStream(label: String?, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
  override fun updateAsciiStream(index: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateAsciiStream(label: String?,
                                 x: InputStream?,
                                 length: Long) = throw SQLFeatureNotSupportedException()

  override fun updateAsciiStream(index: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun updateAsciiStream(label: String?, x: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun updateBytes(index: Int, x: ByteArray?) = throw SQLFeatureNotSupportedException()
  override fun updateBytes(label: String?, x: ByteArray?) = throw SQLFeatureNotSupportedException()
  override fun updateBlob(index: Int, inputStream: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateBlob(label: String?,
                          inputStream: InputStream?,
                          length: Long) = throw SQLFeatureNotSupportedException()

  override fun updateBlob(index: Int, inputStream: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun updateBlob(label: String?, inputStream: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun updateRowId(index: Int, x: RowId?) = throw SQLFeatureNotSupportedException()
  override fun updateRowId(label: String?, x: RowId?) = throw SQLFeatureNotSupportedException()
  override fun updateArray(index: Int, x: java.sql.Array?) = throw SQLFeatureNotSupportedException()
  override fun updateArray(label: String?, x: java.sql.Array?) = throw SQLFeatureNotSupportedException()
  override fun updateSQLXML(index: Int, xmlObject: SQLXML?) = throw SQLFeatureNotSupportedException()
  override fun updateSQLXML(label: String?, xmlObject: SQLXML?) = throw SQLFeatureNotSupportedException()
  override fun updateDate(index: Int, x: Date?) = throw SQLFeatureNotSupportedException()
  override fun updateDate(label: String?, x: Date?) = throw SQLFeatureNotSupportedException()
  override fun updateDouble(index: Int, x: Double) = throw SQLFeatureNotSupportedException()
  override fun updateDouble(label: String?, x: Double) = throw SQLFeatureNotSupportedException()
  override fun updateBlob(index: Int, x: Blob?) = throw SQLFeatureNotSupportedException()
  override fun updateBlob(label: String?, x: Blob?) = throw SQLFeatureNotSupportedException()
  override fun updateByte(index: Int, x: Byte) = throw SQLFeatureNotSupportedException()
  override fun updateByte(label: String?, x: Byte) = throw SQLFeatureNotSupportedException()
  override fun updateRow() = throw SQLFeatureNotSupportedException()
  override fun deleteRow() = throw SQLFeatureNotSupportedException()
  override fun updateFloat(index: Int, x: Float) = throw SQLFeatureNotSupportedException()
  override fun updateFloat(label: String?, x: Float) = throw SQLFeatureNotSupportedException()
}