package com.landoop.jdbc4

import com.landoop.rest.domain.PreparedInsertInfo
import org.apache.avro.Schema
import java.sql.ParameterMetaData
import java.sql.SQLException

/**
 * Implementation of [[ParameterMetaData]] that uses an avro based schema for the parameters.
 */
class AvroSchemaParameterMetaData(val info: PreparedInsertInfo) : ParameterMetaData {

  val valueSchema = Schema.Parser().parse(info.valueSchema)

  private fun field(param: Int): Schema.Field {
    return if (param == 1) {
      // info.fields.find { it.isKey }
      throw RuntimeException()
    } else {
      val name = info.fields[param - 1].name
      return valueSchema.fields.find { it.name() == name }
          ?: throw SQLException("Could not find field $name in ${valueSchema.fields}")
    }
  }

  override fun isNullable(param: Int): Int {
    return if (param == 1) {
      ParameterMetaData.parameterNoNulls
    } else if (field(param).schema().isNullable()) ParameterMetaData.parameterNullable else ParameterMetaData.parameterNoNulls
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

  // plus one to account for the key field
  override fun getParameterCount(): Int = info.fields.size + 1

  override fun getParameterMode(param: Int): Int = ParameterMetaData.parameterModeIn

  override fun getParameterClassName(param: Int): String = AvroSchemas.jvmClassName(field(param).schema().fromUnion().type)
  override fun getParameterType(param: Int): Int = AvroSchemas.sqlType(field(param).schema().fromUnion())
  override fun getParameterTypeName(param: Int): String = field(param).schema().fromUnion().type.name
}
