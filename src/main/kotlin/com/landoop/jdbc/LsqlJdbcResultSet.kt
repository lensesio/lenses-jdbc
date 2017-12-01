package com.landoop.jdbc

import com.landoop.jdbc.domain.JdbcData
import org.apache.avro.Schema
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Date
import java.util.*

class LsqlJdbcResultSet
@Throws(SQLException::class)
protected constructor(private val statement: LsqlJdbcStatement,
                      jdbcData: JdbcData,
                      schema:Schema,
                      type: Int,
                      concurrency: Int,
                      holdability: Int) : ResultSet {

  private var records: List<LsqlResult>
  private var result: LsqlResult? = null

  private var cursor = -1
  private var rowCount = 0
  private var type: Int = 0
  private var concurrency: Int = 0
  private var holdability: Int = 0

  private var resultSetMetaData: LsqlJdbcResultSetMetaData

  init {
    try {
      records = resultSet.toList
    } catch (e: Exception) {
      throw SQLException("Error occourred while mapping results ", e)
    }

    rowCount = records.size

    if (rowCount >= 1) {
      result = records[0]
    } else {
      result = LsqlResult()
    }

    if (type == ResultSet.TYPE_FORWARD_ONLY || type == ResultSet.TYPE_SCROLL_INSENSITIVE || type == ResultSet.TYPE_SCROLL_SENSITIVE)
      this.type = type
    else
      throw SQLException("Bad ResultSet type: " + type + " instead of one of the following values: " + ResultSet.TYPE_FORWARD_ONLY + ", "
          + ResultSet.TYPE_SCROLL_INSENSITIVE + " or" + ResultSet.TYPE_SCROLL_SENSITIVE)

    if (concurrency == ResultSet.CONCUR_READ_ONLY || concurrency == ResultSet.CONCUR_UPDATABLE)
      this.concurrency = concurrency
    else
      throw SQLException(
          "Bad ResultSet Concurrency type: $concurrency  instead of one of the following values: ${ResultSet.CONCUR_READ_ONLY} or ${ResultSet.CONCUR_UPDATABLE}")

    if (holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT || holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT)
      this.holdability = holdability
    else
      throw SQLException(
          """
            Bad ResultSet Holdability type: $holdability instead of one of the
            following values: ${ResultSet.HOLD_CURSORS_OVER_COMMIT} or ${ResultSet.CLOSE_CURSORS_AT_COMMIT}
          """.trimIndent())

    resultSetMetaData = LsqlJdbcResultSetMetaData(this, schema)
  }

  @Throws(SQLException::class)
  override fun close() {
    cursor = 0
    rowCount = 0
    records = emptyList()
  }

  @Throws(SQLException::class)
  override fun first(): Boolean {
    return absolute(0)
  }

  @Throws(SQLException::class)
  override fun last(): Boolean {
    return absolute(rowCount - 1)
  }

  @Throws(SQLException::class)
  override fun next(): Boolean {
    return absolute(++cursor)
  }

  @Throws(SQLException::class)
  override fun previous(): Boolean {
    return absolute(++cursor)
  }

  @Throws(SQLException::class)
  override fun afterLast() {
    // OUT OF LAST ITEM
    cursor = rowCount
  }

  @Throws(SQLException::class)
  override fun beforeFirst() {
    // OUT OF FIRST ITEM
    cursor = -1
  }

  @Throws(SQLException::class)
  override fun relative(iRows: Int): Boolean {
    return absolute(cursor + iRows)
  }

  @Throws(SQLException::class)
  override fun absolute(iRowNumber: Int): Boolean {
    if (iRowNumber > rowCount - 1) {
      cursor = rowCount
      return false
    } else if (iRowNumber < 0) {
      cursor = -1
      return false
    }

    cursor = iRowNumber
    result = records[cursor]
    return true
  }

  @Throws(SQLException::class)
  override fun isAfterLast(): Boolean {
    return cursor >= rowCount - 1
  }

  @Throws(SQLException::class)
  override fun isBeforeFirst(): Boolean {
    return cursor < 0
  }

  @Throws(SQLException::class)
  override fun isClosed(): Boolean {
    return false
  }

  @Throws(SQLException::class)
  override fun isFirst(): Boolean {
    return cursor == 0
  }

  @Throws(SQLException::class)
  override fun isLast(): Boolean {
    return cursor == rowCount - 1
  }

  @Throws(SQLException::class)
  override fun getStatement(): Statement {
    return statement
  }

  @Throws(SQLException::class)
  override fun getMetaData(): ResultSetMetaData {
    return resultSetMetaData
  }

  @Throws(SQLException::class)
  override fun deleteRow() {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun findColumn(columnLabel: String): Int {
    val index = fieldNames.indexOf(columnLabel)
    if (index < 0) {
      throw SQLException("The column '$columnLabel' does not exists. Available column(-s):${fieldNames.joinToString(",")}")
    }

    return index + 1
  }

  @Throws(SQLException::class)
  private fun getFieldIndex(columnIndex: Int): Int {
    if (columnIndex < 1)
      throw SQLException("The column index cannot be less than 1")
    return columnIndex - 1
  }

  @Throws(SQLException::class)
  override fun getArray(columnIndex: Int): java.sql.Array {
    return getArray(fieldNames[getFieldIndex(columnIndex)])

  }

  @Throws(SQLException::class)
  override fun getArray(columnLabel: String): java.sql.Array {
    return LsqlJdbcArray(emptyList())
  }

  @Throws(SQLException::class)
  override fun getAsciiStream(columnIndex: Int): InputStream? {
    return null
  }

  @Throws(SQLException::class)
  override fun getAsciiStream(columnLabel: String): InputStream? {
    return null
  }

  @Throws(SQLException::class)
  override fun getBigDecimal(columnIndex: Int): BigDecimal {

    return getBigDecimal(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getBigDecimal(columnLabel: String): BigDecimal {
    try {
      return result!!.getProperty(columnLabel)
    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the double value at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal {
    return getBigDecimal(fieldNames[getFieldIndex(columnIndex)], scale)
  }

  @Throws(SQLException::class)
  override fun getBigDecimal(columnLabel: String, scale: Int): BigDecimal {
    try {
      return (result!!.getProperty(columnLabel) as BigDecimal).setScale(scale)
    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the double value at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getBinaryStream(columnIndex: Int): InputStream? {
    return getBinaryStream(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getBinaryStream(columnLabel: String): InputStream? {
    try {
      val blob = getBlob(columnLabel)
      return blob?.binaryStream
    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the binary stream at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getBlob(columnIndex: Int): Blob? {
    return getBlob(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getBlob(columnLabel: String): Blob? {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun getBoolean(columnIndex: Int): Boolean {
    return getBoolean(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getBoolean(columnLabel: String): Boolean {
    try {
      return result!!.getProperty(columnLabel)
    } catch (e: Exception) {
      throw SQLException(
          "An error occurred during the retrieval of the boolean value at column '$columnLabel'. " + result!!.toElement().toJSON(), e)
    }

  }

  @Throws(SQLException::class)
  override fun getByte(columnIndex: Int): Byte {
    return getByte(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getByte(columnLabel: String): Byte {
    try {
      return result!!.getProperty(columnLabel)
    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the byte value at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getBytes(columnIndex: Int): ByteArray? {
    return getBytes(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getBytes(columnLabel: String): ByteArray? {
    return result!!.getProperty(columnLabel)
  }

  @Throws(SQLException::class)
  override fun getCharacterStream(columnIndex: Int): Reader? {
    return null
  }

  @Throws(SQLException::class)
  override fun getCharacterStream(columnLabel: String): Reader? {
    return null
  }

  @Throws(SQLException::class)
  override fun getClob(columnIndex: Int): Clob? {
    return null
  }

  @Throws(SQLException::class)
  override fun getClob(columnLabel: String): Clob? {
    return null
  }

  @Throws(SQLException::class)
  override fun getConcurrency(): Int {
    return concurrency
  }

  @Throws(SQLException::class)
  override fun getCursorName(): String? {
    return null
  }

  @Throws(SQLException::class)
  override fun getDate(columnIndex: Int): Date? {
    return getDate(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getDate(columnLabel: String): Date? {
    try {
      val date = result!!.getProperty(columnLabel)
      return if (date != null) Date(date!!.getTime()) else null
    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the date value at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getDate(columnIndex: Int, cal: Calendar): Date? {
    return getDate(fieldNames[getFieldIndex(columnIndex)], cal)
  }

  @Throws(SQLException::class)
  override fun getDate(columnLabel: String, cal: Calendar?): Date? {
    if (cal == null)
      throw SQLException()
    try {
      val date: java.util.Date = result!!.getProperty(columnLabel) ?: return null
      cal.timeInMillis = date.getTime()
      return Date(cal.timeInMillis)
    } catch (e: Exception) {
      throw SQLException(
          "An error occurred during the retrieval of the date value (calendar) at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getDouble(columnIndex: Int): Double {
    val fieldIndex = getFieldIndex(columnIndex)
    return getDouble(fieldNames[fieldIndex])
  }

  @Throws(SQLException::class)
  override fun getDouble(columnLabel: String): Double {
    try {
      val r = result!!.getProperty(columnLabel)
      return r ?: 0
    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the double value at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getFetchDirection(): Int {
    return 0
  }

  @Throws(SQLException::class)
  override fun setFetchDirection(direction: Int) {

  }

  @Throws(SQLException::class)
  override fun getFetchSize(): Int {
    return rowCount
  }

  @Throws(SQLException::class)
  override fun setFetchSize(rows: Int) {

  }

  @Throws(SQLException::class)
  override fun getFloat(columnIndex: Int): Float {

    return getFloat(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getFloat(columnLabel: String): Float {
    try {
      val r = result!!.getProperty(columnLabel)
      return r ?: 0
    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the float value at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getHoldability(): Int {
    return holdability
  }

  @Throws(SQLException::class)
  override fun getInt(columnIndex: Int): Int {
    return getInt(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getInt(columnLabel: String): Int {
    if ("@version" == columnLabel)
      return result!!.toElement().getVersion()

    try {
      val r = result!!.getProperty(columnLabel)
      return r ?: 0

    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the integer value at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getLong(columnIndex: Int): Long {
    return getLong(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getLong(columnLabel: String): Long {

    try {
      val r = result!!.getProperty(columnLabel)
      return r ?: 0
    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the long value at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getNCharacterStream(columnIndex: Int): Reader? {

    return null
  }

  @Throws(SQLException::class)
  override fun getNCharacterStream(columnLabel: String): Reader? {

    return null
  }

  @Throws(SQLException::class)
  override fun getNClob(columnIndex: Int): NClob? {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun getNClob(columnLabel: String): NClob? {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun getNString(columnIndex: Int): String {
    return getNString(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getNString(columnLabel: String): String {
    try {
      return result!!.getProperty(columnLabel)
    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the string value at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getObject(columnIndex: Int): Any? {
    return getObject(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getObject(columnLabel: String): Any? {
    return result!!.getProperty(columnLabel)
  }

  @Throws(SQLException::class)
  override fun getObject(columnIndex: Int, map: Map<String, Class<*>>): Any {
    throw SQLFeatureNotSupportedException("This method has not been implemented.")
  }

  @Throws(SQLException::class)
  override fun getObject(columnLabel: String, map: Map<String, Class<*>>): Any {
    throw SQLFeatureNotSupportedException("This method has not been implemented.")
  }

  @Throws(SQLException::class)
  override fun getRef(columnIndex: Int): Ref? {

    return null
  }

  @Throws(SQLException::class)
  override fun getRef(columnLabel: String): Ref? {
    return null
  }

  @Throws(SQLException::class)
  override fun getRow(): Int {
    return cursor
  }

  @Throws(SQLException::class)
  override fun getRowId(columnIndex: Int): RowId {
    try {
      return OrientRowId(result!!.toElement().getIdentity())
    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the rowid for record '$result'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getRowId(columnLabel: String): RowId {
    return getRowId(0)
  }

  @Throws(SQLException::class)
  override fun getSQLXML(columnIndex: Int): SQLXML? {

    return null
  }

  @Throws(SQLException::class)
  override fun getSQLXML(columnLabel: String): SQLXML? {

    return null
  }

  @Throws(SQLException::class)
  override fun getShort(columnIndex: Int): Short {

    return getShort(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getShort(columnLabel: String): Short {
    try {
      val r = result!!.getProperty(columnLabel)
      return r ?: 0

    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the short value at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getString(columnIndex: Int): String? {

    return getString(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getString(columnLabel: String): String? {

    if ("@rid" == columnLabel || "rid" == columnLabel) {
      return result!!.toElement().getIdentity().toString()
    }

    if ("@class" == columnLabel || "class" == columnLabel) {

      return result!!.toElement().getSchemaType()
          .map({ c -> c.getName() })
          .orElse("NOCLASS")
    }

    try {
      return Optional.ofNullable(result!!.getProperty(columnLabel))
          .map({ v -> "" + v })
          .orElse(null)

    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the string value at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getTime(columnIndex: Int): Time? {
    return getTime(fieldNames[getFieldIndex(columnIndex)])
  }

  @Throws(SQLException::class)
  override fun getTime(columnLabel: String): Time? {
    try {
      val date = result!!.getProperty(columnLabel)
      return getTime(date)
    } catch (e: Exception) {
      throw SQLException("An error occurred during the retrieval of the time value at column '$columnLabel'", e)
    }

  }

  @Throws(SQLException::class)
  override fun getTime(columnIndex: Int, cal: Calendar): Time? {
    val date = getDate(columnIndex, cal)
    return getTime(date)
  }

  private fun getTime(date: java.util.Date?): Time? {
    return if (date != null) Time(date.time) else null
  }

  @Throws(SQLException::class)
  override fun getTime(columnLabel: String, cal: Calendar): Time? {
    val date = getDate(columnLabel, cal)
    return getTime(date)
  }

  @Throws(SQLException::class)
  override fun getTimestamp(columnIndex: Int): Timestamp? {
    val date = getDate(columnIndex)
    return getTimestamp(date)
  }

  private fun getTimestamp(date: Date?): Timestamp? {
    return if (date != null) Timestamp(date.time) else null
  }

  @Throws(SQLException::class)
  override fun getTimestamp(columnLabel: String): Timestamp? {
    val date = getDate(columnLabel)
    return getTimestamp(date)
  }

  @Throws(SQLException::class)
  override fun getTimestamp(columnIndex: Int, cal: Calendar): Timestamp? {
    val date = getDate(columnIndex, cal)
    return getTimestamp(date)
  }

  @Throws(SQLException::class)
  override fun getTimestamp(columnLabel: String, cal: Calendar): Timestamp? {
    val date = getDate(columnLabel, cal)
    return getTimestamp(date)
  }

  @Throws(SQLException::class)
  override fun getType(): Int {
    return type
  }

  @Throws(SQLException::class)
  override fun getURL(columnIndex: Int): URL? {

    return null
  }

  @Throws(SQLException::class)
  override fun getURL(columnLabel: String): URL? {

    return null
  }

  @Throws(SQLException::class)
  override fun getUnicodeStream(columnIndex: Int): InputStream? {

    return null
  }

  @Throws(SQLException::class)
  override fun getUnicodeStream(columnLabel: String): InputStream? {

    return null
  }

  @Throws(SQLException::class)
  override fun getWarnings(): SQLWarning? {

    return null
  }

  @Throws(SQLException::class)
  override fun insertRow() {

  }

  @Throws(SQLException::class)
  override fun moveToCurrentRow() {

  }

  @Throws(SQLException::class)
  override fun moveToInsertRow() {

  }

  @Throws(SQLException::class)
  override fun refreshRow() {

  }

  @Throws(SQLException::class)
  override fun rowDeleted(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun rowInserted(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun rowUpdated(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun updateArray(columnIndex: Int, x: java.sql.Array) {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun updateArray(columnLabel: String, x: java.sql.Array) {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun updateAsciiStream(columnIndex: Int, x: InputStream) {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun updateAsciiStream(columnLabel: String, x: InputStream) {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun updateAsciiStream(columnIndex: Int, x: InputStream, length: Int) {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun updateAsciiStream(columnLabel: String, x: InputStream, length: Int) {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun updateAsciiStream(columnIndex: Int, x: InputStream, length: Long) {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun updateAsciiStream(columnLabel: String, x: InputStream, length: Long) {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun updateBigDecimal(columnIndex: Int, x: BigDecimal) {

  }

  @Throws(SQLException::class)
  override fun updateBigDecimal(columnLabel: String, x: BigDecimal) {

  }

  @Throws(SQLException::class)
  override fun updateBinaryStream(columnIndex: Int, x: InputStream) {

  }

  @Throws(SQLException::class)
  override fun updateBinaryStream(columnLabel: String, x: InputStream) {

  }

  @Throws(SQLException::class)
  override fun updateBinaryStream(columnIndex: Int, x: InputStream, length: Int) {

  }

  @Throws(SQLException::class)
  override fun updateBinaryStream(columnLabel: String, x: InputStream, length: Int) {

  }

  @Throws(SQLException::class)
  override fun updateBinaryStream(columnIndex: Int, x: InputStream, length: Long) {

  }

  @Throws(SQLException::class)
  override fun updateBinaryStream(columnLabel: String, x: InputStream, length: Long) {

  }

  @Throws(SQLException::class)
  override fun updateBlob(columnIndex: Int, x: Blob) {

  }

  @Throws(SQLException::class)
  override fun updateBlob(columnLabel: String, x: Blob) {

  }

  @Throws(SQLException::class)
  override fun updateBlob(columnIndex: Int, inputStream: InputStream) {

  }

  @Throws(SQLException::class)
  override fun updateBlob(columnLabel: String, inputStream: InputStream) {

  }

  @Throws(SQLException::class)
  override fun updateBlob(columnIndex: Int, inputStream: InputStream, length: Long) {

  }

  @Throws(SQLException::class)
  override fun updateBlob(columnLabel: String, inputStream: InputStream, length: Long) {

  }

  @Throws(SQLException::class)
  override fun updateBoolean(columnIndex: Int, x: Boolean) {

  }

  @Throws(SQLException::class)
  override fun updateBoolean(columnLabel: String, x: Boolean) {

  }

  @Throws(SQLException::class)
  override fun updateByte(columnIndex: Int, x: Byte) {

  }

  @Throws(SQLException::class)
  override fun updateByte(columnLabel: String, x: Byte) {

  }

  @Throws(SQLException::class)
  override fun updateBytes(columnIndex: Int, x: ByteArray) {

  }

  @Throws(SQLException::class)
  override fun updateBytes(columnLabel: String, x: ByteArray) {

  }

  @Throws(SQLException::class)
  override fun updateCharacterStream(columnIndex: Int, x: Reader) {

  }

  @Throws(SQLException::class)
  override fun updateCharacterStream(columnLabel: String, reader: Reader) {

  }

  @Throws(SQLException::class)
  override fun updateCharacterStream(columnIndex: Int, x: Reader, length: Int) {

  }

  @Throws(SQLException::class)
  override fun updateCharacterStream(columnLabel: String, reader: Reader, length: Int) {

  }

  @Throws(SQLException::class)
  override fun updateCharacterStream(columnIndex: Int, x: Reader, length: Long) {

  }

  @Throws(SQLException::class)
  override fun updateCharacterStream(columnLabel: String, reader: Reader, length: Long) {

  }

  @Throws(SQLException::class)
  override fun updateClob(columnIndex: Int, x: Clob) {

  }

  @Throws(SQLException::class)
  override fun updateClob(columnLabel: String, x: Clob) {

  }

  @Throws(SQLException::class)
  override fun updateClob(columnIndex: Int, reader: Reader) {

  }

  @Throws(SQLException::class)
  override fun updateClob(columnLabel: String, reader: Reader) {

  }

  @Throws(SQLException::class)
  override fun updateClob(columnIndex: Int, reader: Reader, length: Long) {

  }

  @Throws(SQLException::class)
  override fun updateClob(columnLabel: String, reader: Reader, length: Long) {

  }

  @Throws(SQLException::class)
  override fun updateDate(columnIndex: Int, x: Date) {

  }

  @Throws(SQLException::class)
  override fun updateDate(columnLabel: String, x: Date) {

  }

  @Throws(SQLException::class)
  override fun updateDouble(columnIndex: Int, x: Double) {

  }

  @Throws(SQLException::class)
  override fun updateDouble(columnLabel: String, x: Double) {

  }

  @Throws(SQLException::class)
  override fun updateFloat(columnIndex: Int, x: Float) {

  }

  @Throws(SQLException::class)
  override fun updateFloat(columnLabel: String, x: Float) {

  }

  @Throws(SQLException::class)
  override fun updateInt(columnIndex: Int, x: Int) {

  }

  @Throws(SQLException::class)
  override fun updateInt(columnLabel: String, x: Int) {

  }

  @Throws(SQLException::class)
  override fun updateLong(columnIndex: Int, x: Long) {

  }

  @Throws(SQLException::class)
  override fun updateLong(columnLabel: String, x: Long) {

  }

  @Throws(SQLException::class)
  override fun updateNCharacterStream(columnIndex: Int, x: Reader) {

  }

  @Throws(SQLException::class)
  override fun updateNCharacterStream(columnLabel: String, reader: Reader) {

  }

  @Throws(SQLException::class)
  override fun updateNCharacterStream(columnIndex: Int, x: Reader, length: Long) {

  }

  @Throws(SQLException::class)
  override fun updateNCharacterStream(columnLabel: String, reader: Reader, length: Long) {

  }

  @Throws(SQLException::class)
  override fun updateNClob(columnIndex: Int, nClob: NClob) {

  }

  @Throws(SQLException::class)
  override fun updateNClob(columnLabel: String, nClob: NClob) {

  }

  @Throws(SQLException::class)
  override fun updateNClob(columnIndex: Int, reader: Reader) {

  }

  @Throws(SQLException::class)
  override fun updateNClob(columnLabel: String, reader: Reader) {

  }

  @Throws(SQLException::class)
  override fun updateNClob(columnIndex: Int, reader: Reader, length: Long) {

  }

  @Throws(SQLException::class)
  override fun updateNClob(columnLabel: String, reader: Reader, length: Long) {

  }

  @Throws(SQLException::class)
  override fun updateNString(columnIndex: Int, nString: String) {

  }

  @Throws(SQLException::class)
  override fun updateNString(columnLabel: String, nString: String) {

  }

  @Throws(SQLException::class)
  override fun updateNull(columnIndex: Int) {

  }

  @Throws(SQLException::class)
  override fun updateNull(columnLabel: String) {

  }

  @Throws(SQLException::class)
  override fun updateObject(columnIndex: Int, x: Any) {

  }

  @Throws(SQLException::class)
  override fun updateObject(columnLabel: String, x: Any) {

  }

  @Throws(SQLException::class)
  override fun updateObject(columnIndex: Int, x: Any, scaleOrLength: Int) {

  }

  @Throws(SQLException::class)
  override fun updateObject(columnLabel: String, x: Any, scaleOrLength: Int) {

  }

  @Throws(SQLException::class)
  override fun updateRef(columnIndex: Int, x: Ref) {

  }

  @Throws(SQLException::class)
  override fun updateRef(columnLabel: String, x: Ref) {

  }

  @Throws(SQLException::class)
  override fun updateRow() {

  }

  @Throws(SQLException::class)
  override fun updateRowId(columnIndex: Int, x: RowId) {

  }

  @Throws(SQLException::class)
  override fun updateRowId(columnLabel: String, x: RowId) {

  }

  @Throws(SQLException::class)
  override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML) {

  }

  @Throws(SQLException::class)
  override fun updateSQLXML(columnLabel: String, xmlObject: SQLXML) {

  }

  @Throws(SQLException::class)
  override fun updateShort(columnIndex: Int, x: Short) {

  }

  @Throws(SQLException::class)
  override fun updateShort(columnLabel: String, x: Short) {

  }

  @Throws(SQLException::class)
  override fun updateString(columnIndex: Int, x: String) {

  }

  @Throws(SQLException::class)
  override fun updateString(columnLabel: String, x: String) {

  }

  @Throws(SQLException::class)
  override fun updateTime(columnIndex: Int, x: Time) {

  }

  @Throws(SQLException::class)
  override fun updateTime(columnLabel: String, x: Time) {

  }

  @Throws(SQLException::class)
  override fun updateTimestamp(columnIndex: Int, x: Timestamp) {

  }

  @Throws(SQLException::class)
  override fun updateTimestamp(columnLabel: String, x: Timestamp) {

  }

  @Throws(SQLException::class)
  override fun wasNull(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun isWrapperFor(iface: Class<*>): Boolean {
    return ODocument::class.java!!.isAssignableFrom(iface)
  }

  @Throws(SQLException::class)
  override fun <T> unwrap(iface: Class<T>): T? {
    try {
      return iface.cast(result)
    } catch (e: ClassCastException) {
      throw SQLException(e)
    }

  }

  @Throws(SQLException::class)
  override fun cancelRowUpdates() {
  }

  @Throws(SQLException::class)
  override fun clearWarnings() {
  }

  @Throws(SQLException::class)
  override fun <T> getObject(arg0: Int, arg1: Class<T>): T? {
    return null
  }

  @Throws(SQLException::class)
  override fun <T> getObject(arg0: String, arg1: Class<T>): T? {
    return null
  }
}
