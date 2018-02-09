package com.landoop.jdbc4

import com.landoop.rest.domain.SqlInsertField
import org.apache.avro.Schema
import java.sql.ParameterMetaData
import java.sql.SQLException

class AvroSchemaParameterMetaData(val fields: List<SqlInsertField>, val schema: Schema) : ParameterMetaData {

  private fun field(param: Int): Schema.Field {
    val name = fields[param - 1].name
    return schema.fields.find { it.name() == name } ?: throw SQLException("Could not find field $name in ${schema.fields}")
  }

  override fun isNullable(param: Int): Int {
    return if (field(param).schema().isNullable()) ParameterMetaData.parameterNullable else ParameterMetaData.parameterNoNulls
  }

  override fun isWrapperFor(iface: Class<*>?): Boolean = this.isWrapperFor(iface)
  override fun <T : Any?> unwrap(iface: Class<T>): T {
    try {
      return iface.cast(this)
    } catch (cce: ClassCastException) {
      throw SQLException("Unable to unwrap instance as " + iface.toString())
    }
  }

  override fun isSigned(param: Int): Boolean = field(param).schema().fromUnion().isNumber()

  override fun getPrecision(param: Int): Int = 0
  override fun getScale(param: Int): Int = 0

  override fun getParameterCount(): Int = fields.size
  override fun getParameterMode(param: Int): Int = ParameterMetaData.parameterModeIn

  override fun getParameterClassName(param: Int): String = AvroSchemas.jvmClassName(field(param).schema().fromUnion().type)
  override fun getParameterType(param: Int): Int = AvroSchemas.sqlType(field(param).schema().fromUnion())
  override fun getParameterTypeName(param: Int): String = field(param).schema().fromUnion().type.name
}
