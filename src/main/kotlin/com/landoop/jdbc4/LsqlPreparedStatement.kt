package com.landoop.jdbc4

import com.landoop.rest.RestClient
import com.landoop.rest.domain.PreparedInsertStatementInfo
import org.apache.avro.Schema
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.Blob
import java.sql.Clob
import java.sql.Connection
import java.sql.Date
import java.sql.NClob
import java.sql.ParameterMetaData
import java.sql.PreparedStatement
import java.sql.Ref
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.RowId
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLXML
import java.sql.Time
import java.sql.Timestamp
import java.util.*

class LsqlPreparedStatement(conn: Connection,
                            private val client: RestClient,
                            sql: String) : LsqlStatement(conn, client), PreparedStatement {

  private val undefinedSentinel = "_______________________undefined_lsql"

  // for a prepared statement we need to connect to bring back parameter details,
  // as the endpoint will parse our query
  private val info: PreparedInsertStatementInfo = client.prepareStatement(sql).let {
    it.info ?: throw SQLException(it.error ?: "No error info available")
  }

  // the last resultset retrieved by this statement
  private var rs: ResultSet = RowResultSet.empty()

  // contains the values being set for the current prepared statement
  private var record = createRecord()

  private val batch = mutableListOf<List<Any?>>()

  private fun createRecord(): MutableList<Any?> {
    return ArrayList<Any?>(info.fields.size).apply {
      clearParameters()
    }
  }

  override fun clearParameters() {
    for (k in 0..record.size) {
      record[k] = undefinedSentinel
    }
  }

  override fun execute(): Boolean {
    // we must have set all the required parameters
    if (record.contains(undefinedSentinel)) throw SQLException("You must set all values before executing; if null is desired, explicitly set this using setNull(pos)")
    client.executePrepared(info.topic, listOf(record.toList()))
    return true
  }

  override fun executeQuery(): ResultSet {
    TODO()
  }

  override fun executeUpdate(): Int {
    checkRecord()
    client.executePrepared(info.topic, listOf(record.toList()))
    return 0
  }

  override fun getParameterMetaData(): ParameterMetaData {
    val schema = Schema.Parser().parse(info.valueSchema)
    return AvroSchemaParameterMetaData(info.fields, schema)
  }

  override fun getMetaData(): ResultSetMetaData {
    TODO()
  }

  private fun checkRecord() {
    // we must have set all the required parameters
    if (record.contains(undefinedSentinel)) throw SQLException("You must set all values before executing; if null is desired, explicitly set this using setNull(pos)")
  }

  // todo support batching

  override fun addBatch() {
    checkRecord()
    batch.add(record)
    record = createRecord()
  }

  override fun clearBatch() {
    batch.clear()
  }

  override fun executeBatch(): IntArray {
    client.executePrepared(info.topic, batch)
    return IntArray(1)
  }

  // -- methods which set values on the current record

  override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Int) {
    setCharacterStream(parameterIndex, reader)
  }

  override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Long) {
    setCharacterStream(parameterIndex, reader)
  }

  override fun setCharacterStream(parameterIndex: Int, reader: Reader?) {
    val string = reader!!.readText()
    record[parameterIndex] = string
  }

  override fun setDate(parameterIndex: Int, x: Date?) {
    record[parameterIndex] = x
  }

  override fun setDate(parameterIndex: Int, x: Date?, cal: Calendar?) {
    record[parameterIndex] = x
  }

  override fun setObject(parameterIndex: Int, x: Any?) {
    record[parameterIndex] = x
  }

  override fun setLong(parameterIndex: Int, x: Long) {
    record[parameterIndex] = x
  }

  override fun setNString(parameterIndex: Int, x: String?) {
    record[parameterIndex] = x
  }

  override fun setURL(parameterIndex: Int, x: URL?) {
    record[parameterIndex] = x
  }

  override fun setFloat(parameterIndex: Int, x: Float) {
    record[parameterIndex] = x
  }

  override fun setTime(parameterIndex: Int, x: Time?) {
    record[parameterIndex] = x
  }

  override fun setTime(parameterIndex: Int, x: Time?, cal: Calendar?) {
    record[parameterIndex] = x
  }

  override fun setNCharacterStream(parameterIndex: Int, value: Reader?, length: Long) {
    record[parameterIndex] = value!!.readLines()
  }

  override fun setNCharacterStream(parameterIndex: Int, value: Reader?) {
    record[parameterIndex] = value!!.readLines()
  }

  override fun setInt(parameterIndex: Int, x: Int) {
    record[parameterIndex] = x
  }

  override fun setDouble(parameterIndex: Int, x: Double) {
    record[parameterIndex] = x
  }

  override fun setBigDecimal(parameterIndex: Int, x: BigDecimal?) {
    record[parameterIndex] = x
  }

  override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int) {
    record[parameterIndex] = x
  }

  override fun setString(parameterIndex: Int, x: String?) {
    record[parameterIndex] = x
  }

  override fun setNull(parameterIndex: Int, sqlType: Int) {
    record[parameterIndex] = null
  }

  override fun setNull(parameterIndex: Int, sqlType: Int, typeName: String?) {
    record[parameterIndex] = null
  }

  override fun setTimestamp(parameterIndex: Int, x: Timestamp?) {
    record[parameterIndex] = x
  }

  override fun setTimestamp(parameterIndex: Int, x: Timestamp?, cal: Calendar?) {
    record[parameterIndex] = x
  }

  override fun setShort(parameterIndex: Int, x: Short) {
    record[parameterIndex] = x
  }

  override fun setBoolean(parameterIndex: Int, x: Boolean) {
    record[parameterIndex] = x
  }

  override fun setByte(parameterIndex: Int, x: Byte) {
    record[parameterIndex] = x
  }

  // -- unsupported types

  override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
  override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun setBinaryStream(parameterIndex: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun setClob(parameterIndex: Int, x: Clob?) = throw SQLFeatureNotSupportedException()
  override fun setClob(parameterIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun setClob(parameterIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
  override fun setUnicodeStream(parameterIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
  override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int, scaleOrLength: Int) = throw SQLFeatureNotSupportedException()
  override fun setBytes(parameterIndex: Int, x: ByteArray?) = throw SQLFeatureNotSupportedException()
  override fun setSQLXML(parameterIndex: Int, xmlObject: SQLXML?) = throw SQLFeatureNotSupportedException()
  override fun setRef(parameterIndex: Int, x: Ref?) = throw SQLFeatureNotSupportedException()
  override fun setBlob(parameterIndex: Int, x: Blob?) = throw SQLFeatureNotSupportedException()
  override fun setBlob(parameterIndex: Int, inputStream: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun setBlob(parameterIndex: Int, inputStream: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun setArray(parameterIndex: Int, x: java.sql.Array?) = throw SQLFeatureNotSupportedException()
  override fun setRowId(parameterIndex: Int, x: RowId?) = throw SQLFeatureNotSupportedException()
  override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
  override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun setAsciiStream(parameterIndex: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun setNClob(parameterIndex: Int, value: NClob?) = throw SQLFeatureNotSupportedException()
  override fun setNClob(parameterIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun setNClob(parameterIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()

  // -- execute methods with SQL parameters are not used by prepared statements

  override fun execute(sql: String): Boolean = throw SQLFeatureNotSupportedException("This method cannot be called on a prepared statement")
  override fun addBatch(sql: String?) = throw SQLFeatureNotSupportedException("This method cannot be called on a prepared statement")
  override fun executeQuery(sql: String): ResultSet = throw SQLFeatureNotSupportedException("This method cannot be called on a prepared statement")

  // == the following are methods that update and thus are not supported by this read only jdbc interface ==

  override fun execute(sql: String?, autoGeneratedKeys: Int): Boolean = throw SQLFeatureNotSupportedException("Auto generated keys are not supported by Lenses")
  override fun execute(sql: String?, columnIndexes: IntArray?): Boolean = throw SQLFeatureNotSupportedException("Auto generated keys are not supported by Lenses")
  override fun execute(sql: String?, columnNames: Array<out String>?): Boolean = throw SQLFeatureNotSupportedException("Auto generated keys are not supported by Lenses")
  override fun getGeneratedKeys(): ResultSet = throw SQLFeatureNotSupportedException("Auto generated keys are not supported by Lenses")
}

