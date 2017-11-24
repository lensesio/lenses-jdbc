package com.landoop.jdbc

import java.math.BigDecimal
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Types
import java.util.*

class LsqlJdbcResultSetMetaData : ResultSetMetaData {

}

class OrientJdbcResultSetMetaData(private val resultSet: OrientJdbcResultSet, fieldNames: List<String>) : ResultSetMetaData {

  private val fieldNames: Array<String>

  protected val currentRecord: OResult?
    @Throws(SQLException::class)
    get() = resultSet.unwrap(OResult::class.java) ?: throw SQLException("No current record")

  init {
    this.fieldNames = fieldNames.toTypedArray()
  }

  @Throws(SQLException::class)
  override fun getColumnCount(): Int {

    return fieldNames.size
  }

  @Throws(SQLException::class)
  override fun getCatalogName(column: Int): String {
    // return an empty String according to the method's documentation
    return ""
  }

  @Throws(SQLException::class)
  override fun getColumnClassName(column: Int): String? {
    val value = this.resultSet.getObject(column) ?: return null
    return value.javaClass.canonicalName
  }

  @Throws(SQLException::class)
  override fun getColumnDisplaySize(column: Int): Int {
    return 0
  }

  @Throws(SQLException::class)
  override fun getColumnLabel(column: Int): String {
    return getColumnName(column)
  }

  @Throws(SQLException::class)
  override fun getColumnName(column: Int): String {
    return fieldNames[column - 1]
  }

  @Throws(SQLException::class)
  override fun getColumnType(column: Int): Int {
    val currentRecord = currentRecord

    if (column > fieldNames.size)
      return Types.NULL

    val fieldName = fieldNames[column - 1]

    val otype = currentRecord!!.toElement()
        .getSchemaType()
        .map({ st -> st.getProperty(fieldName) })
        .map({ op -> op.getType() })
        .orElse(null)

    if (otype == null) {
      val value = currentRecord!!.getProperty(fieldName)

      if (value == null) {
        return Types.NULL
      } else if (value is OBlob) {
        // Check if the type is a binary record or a collection of binary
        // records
        return Types.BINARY
      } else if (value is ORecordLazyList) {
        val list = value as ORecordLazyList
        // check if all the list items are instances of ORecordBytes
        val iterator = list!!.listIterator()
        var listElement: OIdentifiable
        var stop = false
        while (iterator.hasNext() && !stop) {
          listElement = iterator.next()
          if (listElement !is OBlob)
            stop = true
        }
        if (!stop) {
          return Types.BLOB
        }
      }
      return getSQLTypeFromJavaClass(value)
    } else {
      if (otype === OType.EMBEDDED || otype === OType.LINK) {
        val value = currentRecord!!.getProperty(fieldName) ?: return Types.NULL
// 1. Check if the type is another record or a collection of records
        if (value is OBlob) {
          return Types.BINARY
        }
      } else {
        if (otype === OType.EMBEDDEDLIST || otype === OType.LINKLIST) {
          val value = currentRecord!!.getProperty(fieldName) ?: return Types.NULL
          if (value is ORecordLazyList) {
            val list = value as ORecordLazyList
            // check if all the list items are instances of ORecordBytes
            val iterator = list!!.listIterator()
            var listElement: OIdentifiable
            var stop = false
            while (iterator.hasNext() && !stop) {
              listElement = iterator.next()
              if (listElement !is OBlob)
                stop = true
            }
            return if (stop) {
              typesSqlTypes[otype]
            } else {
              Types.BLOB
            }
          }
        }
      }
    }
    return typesSqlTypes[otype]
  }

  private fun getSQLTypeFromJavaClass(value: Any): Int {
    return if (value is Boolean)
      typesSqlTypes[OType.BOOLEAN]
    else if (value is Byte)
      typesSqlTypes[OType.BYTE]
    else if (value is Date)
      typesSqlTypes[OType.DATETIME]
    else if (value is Double)
      typesSqlTypes[OType.DOUBLE]
    else if (value is BigDecimal)
      typesSqlTypes[OType.DECIMAL]
    else if (value is Float)
      typesSqlTypes[OType.FLOAT]
    else if (value is Int)
      typesSqlTypes[OType.INTEGER]
    else if (value is Long)
      typesSqlTypes[OType.LONG]
    else if (value is Short)
      typesSqlTypes[OType.SHORT]
    else if (value is String)
      typesSqlTypes[OType.STRING]
    else if (value is List<*>)
      typesSqlTypes[OType.EMBEDDEDLIST]
    else
      Types.JAVA_OBJECT
  }

  @Throws(SQLException::class)
  override fun getColumnTypeName(column: Int): String {
    val currentRecord = currentRecord

    val columnLabel = fieldNames[column - 1]

    return currentRecord!!.toElement().getSchemaType()
        .map({ st -> st.getProperty(columnLabel) })
        .map({ p -> p.getType() })
        .map({ t -> t.toString() })
        .orElse(null)
  }

  @Throws(SQLException::class)
  override fun getPrecision(column: Int): Int {
    return 0
  }

  @Throws(SQLException::class)
  override fun getScale(column: Int): Int {
    return 0
  }

  @Throws(SQLException::class)
  override fun getSchemaName(column: Int): String {
    val currentRecord = currentRecord
    return if (currentRecord == null)
      ""
    else
      currentRecord!!.toElement().getDatabase().getName()
  }

  @Throws(SQLException::class)
  override fun getTableName(column: Int): String? {
    val p = getProperty(column)
    return if (p != null) p!!.getOwnerClass().getName() else null
  }

  @Throws(SQLException::class)
  override fun isAutoIncrement(column: Int): Boolean {
    return false
  }

  @Throws(SQLException::class)
  override fun isCaseSensitive(column: Int): Boolean {
    val p = getProperty(column)
    return if (p != null) p!!.getCollate().getName().equalsIgnoreCase("ci") else false
  }

  @Throws(SQLException::class)
  override fun isCurrency(column: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun isDefinitelyWritable(column: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun isNullable(column: Int): Int {
    return ResultSetMetaData.columnNullableUnknown
  }

  @Throws(SQLException::class)
  override fun isReadOnly(column: Int): Boolean {
    val p = getProperty(column)
    return if (p != null) p!!.isReadonly() else false
  }

  @Throws(SQLException::class)
  override fun isSearchable(column: Int): Boolean {
    return true
  }

  @Throws(SQLException::class)
  override fun isSigned(column: Int): Boolean {
    val currentRecord = currentRecord
    val otype = currentRecord!!.toElement().getSchemaType()
        .map({ st -> st.getProperty(fieldNames[column - 1]).getType() })
        .orElse(null)

    return this.isANumericColumn(otype)
  }

  @Throws(SQLException::class)
  override fun isWritable(column: Int): Boolean {
    return !isReadOnly(column)
  }

  @Throws(SQLException::class)
  override fun isWrapperFor(iface: Class<*>): Boolean {
    return false
  }

  @Throws(SQLException::class)
  override fun <T> unwrap(iface: Class<T>): T? {
    return null
  }

  private fun isANumericColumn(type: OType): Boolean {
    return (type === OType.BYTE
        || type === OType.DOUBLE
        || type === OType.FLOAT
        || type === OType.INTEGER
        || type === OType.LONG
        || type === OType.SHORT)
  }

  @Throws(SQLException::class)
  protected fun getProperty(column: Int): OProperty? {

    val fieldName = getColumnName(column)

    return currentRecord!!.toElement().getSchemaType()
        .map({ st -> st.getProperty(fieldName) })
        .orElse(null)

  }

  companion object {

    private val typesSqlTypes = HashMap<K, V>()

    init {
      typesSqlTypes.put(OType.STRING, Types.VARCHAR)
      typesSqlTypes.put(OType.INTEGER, Types.INTEGER)
      typesSqlTypes.put(OType.FLOAT, Types.FLOAT)
      typesSqlTypes.put(OType.SHORT, Types.SMALLINT)
      typesSqlTypes.put(OType.BOOLEAN, Types.BOOLEAN)
      typesSqlTypes.put(OType.LONG, Types.BIGINT)
      typesSqlTypes.put(OType.DOUBLE, Types.DOUBLE)
      typesSqlTypes.put(OType.DECIMAL, Types.DECIMAL)
      typesSqlTypes.put(OType.DATE, Types.DATE)
      typesSqlTypes.put(OType.DATETIME, Types.TIMESTAMP)
      typesSqlTypes.put(OType.BYTE, Types.TINYINT)
      typesSqlTypes.put(OType.SHORT, Types.SMALLINT)

      // NOT SURE ABOUT THE FOLLOWING MAPPINGS
      typesSqlTypes.put(OType.BINARY, Types.BINARY)
      typesSqlTypes.put(OType.EMBEDDED, Types.JAVA_OBJECT)
      typesSqlTypes.put(OType.EMBEDDEDLIST, Types.ARRAY)
      typesSqlTypes.put(OType.EMBEDDEDMAP, Types.JAVA_OBJECT)
      typesSqlTypes.put(OType.EMBEDDEDSET, Types.ARRAY)
      typesSqlTypes.put(OType.LINK, Types.JAVA_OBJECT)
      typesSqlTypes.put(OType.LINKLIST, Types.ARRAY)
      typesSqlTypes.put(OType.LINKMAP, Types.JAVA_OBJECT)
      typesSqlTypes.put(OType.LINKSET, Types.ARRAY)
      typesSqlTypes.put(OType.TRANSIENT, Types.NULL)
    }

    fun getSqlType(iType: OType): Int? {
      return typesSqlTypes[iType]
    }
  }

}
