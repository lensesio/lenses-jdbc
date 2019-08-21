package io.lenses.jdbc4.resultset

import java.io.InputStream
import java.io.Reader
import java.net.URL
import java.sql.Blob
import java.sql.Clob
import java.sql.NClob
import java.sql.Ref
import java.sql.ResultSet
import java.sql.RowId
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLXML

interface UnsupportedTypesResultSet : ResultSet {
  override fun getNClob(index: Int): NClob = throw SQLFeatureNotSupportedException()
  override fun getNClob(label: String?): NClob = throw SQLFeatureNotSupportedException()
  override fun getBinaryStream(index: Int): InputStream? = throw SQLFeatureNotSupportedException()
  override fun getBinaryStream(label: String): InputStream? = throw SQLFeatureNotSupportedException()
  override fun getBlob(index: Int): Blob? = throw SQLFeatureNotSupportedException()
  override fun getBlob(label: String): Blob? = throw SQLFeatureNotSupportedException()
  override fun getUnicodeStream(index: Int): InputStream = throw SQLFeatureNotSupportedException()
  override fun getUnicodeStream(label: String?): InputStream = throw SQLFeatureNotSupportedException()
  override fun getNCharacterStream(index: Int): Reader = throw SQLFeatureNotSupportedException()
  override fun getNCharacterStream(label: String?): Reader = throw SQLFeatureNotSupportedException()
  override fun getAsciiStream(index: Int): InputStream = throw SQLFeatureNotSupportedException()
  override fun getAsciiStream(label: String?): InputStream = throw SQLFeatureNotSupportedException()
  override fun getSQLXML(index: Int): SQLXML = throw SQLFeatureNotSupportedException()
  override fun getSQLXML(label: String?): SQLXML = throw SQLFeatureNotSupportedException()
  override fun getURL(index: Int): URL = throw SQLFeatureNotSupportedException()
  override fun getURL(label: String?): URL = throw SQLFeatureNotSupportedException()
  override fun getObject(index: Int, map: MutableMap<String, Class<*>>?): Any = throw SQLFeatureNotSupportedException()
  override fun getObject(label: String?,map: MutableMap<String, Class<*>>?): Any = throw SQLFeatureNotSupportedException()
  override fun <T : Any?> getObject(index: Int, type: Class<T>?): T = throw SQLFeatureNotSupportedException()
  override fun <T : Any?> getObject(label: String?, type: Class<T>?): T = throw SQLFeatureNotSupportedException()
  override fun getClob(index: Int): Clob = throw SQLFeatureNotSupportedException()
  override fun getClob(label: String?): Clob = throw SQLFeatureNotSupportedException()
  override fun getArray(index: Int): java.sql.Array = throw SQLFeatureNotSupportedException()
  override fun getArray(label: String?): java.sql.Array = throw SQLFeatureNotSupportedException()
  override fun getRef(index: Int): Ref = throw SQLFeatureNotSupportedException()
  override fun getRef(label: String?): Ref = throw SQLFeatureNotSupportedException()

  override fun getRowId(index: Int): RowId = throw SQLFeatureNotSupportedException()
  override fun getRowId(label: String?): RowId = throw SQLFeatureNotSupportedException()
}