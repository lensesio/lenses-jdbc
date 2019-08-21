package io.lenses.jdbc4

import io.lenses.jdbc4.client.domain.PreparedInsertInfo
import org.apache.avro.Schema
import java.sql.ParameterMetaData

/**
 * Implementation of [[ParameterMetaData]] that uses an avro based schema for the parameters.
 */
class AvroSchemaParameterMetaData(val info: PreparedInsertInfo) : ParameterMetaData, io.lenses.jdbc4.IWrapper {

  val valueSchema: Schema = Schema.Parser().parse(info.valueSchema)

  override fun isNullable(param: Int): Int {
    return if (info.isKey(param - 1)) {
      ParameterMetaData.parameterNoNulls
    } else {
      if (info.schemaField(param - 1).schema().isNullable) {
        ParameterMetaData.parameterNullable
      } else {
        ParameterMetaData.parameterNoNulls
      }
    }
  }

  override fun isWrapperFor(iface: Class<*>?): Boolean = _isWrapperFor(iface)
  override fun <T : Any?> unwrap(iface: Class<T>): T = _unwrap(iface)

  override fun isSigned(param: Int): Boolean = info.schemaField(param - 1).schema().fromUnion().isNumber()

  override fun getPrecision(param: Int): Int = 0
  override fun getScale(param: Int): Int = 0

  override fun getParameterCount(): Int = info.fields.size

  // we don't support stored procs, so always in-mode
  override fun getParameterMode(param: Int): Int = ParameterMetaData.parameterModeIn

  override fun getParameterClassName(param: Int): String = AvroSchemas.jvmClassName(info.schemaField(
      param - 1).schema().fromUnion().type)
  override fun getParameterType(param: Int): Int = AvroSchemas.sqlType(info.schemaField(param - 1).schema().fromUnion())
  override fun getParameterTypeName(param: Int): String = info.schemaField(param - 1).schema().fromUnion().type.name
}
