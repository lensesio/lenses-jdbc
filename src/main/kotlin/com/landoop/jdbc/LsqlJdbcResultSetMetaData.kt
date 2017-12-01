package com.landoop.jdbc

import com.landoop.jdbc.avro.toJdbcFields
import com.landoop.jdbc.domain.JdbcField
import org.apache.avro.Schema
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Types

class LsqlJdbcResultSetMetaData(private val resultSet: LsqlJdbcResultSet,
                                private val table: String?,
                                schema: Schema) : ResultSetMetaData {

  private val fields: List<JdbcField>
  private val fieldsIndexMap: Map<String, Int>

  protected val currentRecord: LsqlResult?
    @Throws(SQLException::class)
    get() = resultSet.unwrap(LsqlResult::class.java) ?: throw SQLException("No current record")

  init {
    fields = schema.toJdbcFields()
    fieldsIndexMap = fields.map { it -> it.name to it.index }.toMap()
  }

  @Throws(SQLException::class)
  override fun getColumnCount(): Int = fields.size

  // return an empty String according to the method's documentation
  @Throws(SQLException::class)
  override fun getCatalogName(column: Int): String = ""

  @Throws(SQLException::class)
  override fun getColumnClassName(column: Int): String? {
    val value = this.resultSet.getObject(column) ?: return null
    return value.javaClass.canonicalName
  }

  @Throws(SQLException::class)
  override fun getColumnDisplaySize(column: Int): Int = 0

  @Throws(SQLException::class)
  override fun getColumnLabel(column: Int): String = getColumnName(column)

  @Throws(SQLException::class)
  override fun getColumnName(column: Int): String = fields[column - 1].name


  @Throws(SQLException::class)
  override fun getColumnType(column: Int): Int {
    require(column > 0 && column <= fields.size, { "Column index of $column is out of bounds. Should be between 1 and ${fields.size}" })
    val jdbcField = fields[column - 1]
    return jdbcField.jdbcType
  }

  @Throws(SQLException::class)
  override fun getColumnTypeName(column: Int): String {
    return when (getColumnType(column)) {
      Types.BOOLEAN -> "BOOLEAN"
      Types.BINARY -> "BINARY"
      Types.DOUBLE -> "DOUBLE"
      Types.FLOAT -> "FLOAT"
      Types.BIGINT -> "BIGINT"
      Types.DECIMAL -> "DECIMAL"
      Types.DATE -> "DATE"
      else -> "VARCHAR"
    }
  }

  @Throws(SQLException::class)
  override fun getPrecision(column: Int): Int = 0

  @Throws(SQLException::class)
  override fun getScale(column: Int): Int = 0

  @Throws(SQLException::class)
  override fun getSchemaName(column: Int): String = Constants.DatabaseName

  @Throws(SQLException::class)
  override fun getTableName(column: Int): String? = table

  @Throws(SQLException::class)
  override fun isAutoIncrement(column: Int): Boolean = false

  @Throws(SQLException::class)
  override fun isCaseSensitive(column: Int): Boolean = true

  @Throws(SQLException::class)
  override fun isCurrency(column: Int): Boolean = false

  @Throws(SQLException::class)
  override fun isDefinitelyWritable(column: Int): Boolean = false

  @Throws(SQLException::class)
  override fun isNullable(column: Int): Int = ResultSetMetaData.columnNullableUnknown


  @Throws(SQLException::class)
  override fun isReadOnly(column: Int): Boolean = true

  @Throws(SQLException::class)
  override fun isSearchable(column: Int): Boolean = true

  @Throws(SQLException::class)
  override fun isSigned(column: Int): Boolean = this.isANumericColumn(getColumnType(column))


  @Throws(SQLException::class)
  override fun isWritable(column: Int): Boolean = false

  @Throws(SQLException::class)
  override fun isWrapperFor(iface: Class<*>): Boolean = false

  @Throws(SQLException::class)
  override fun <T> unwrap(iface: Class<T>): T? = null

  private fun isANumericColumn(type: Int): Boolean {
    return type == Types.BIGINT ||
        type == Types.DECIMAL ||
        type == Types.DOUBLE ||
        type == Types.FLOAT ||
        type == Types.INTEGER ||
        type == Types.SMALLINT
  }
}
