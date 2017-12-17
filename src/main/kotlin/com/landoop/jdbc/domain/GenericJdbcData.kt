package com.landoop.jdbc.domain

import org.apache.avro.Schema

class GenericJdbcData(private var data: Array<Array<Any?>>,
                      override val schema: Schema,
                      override val table: String?) : JdbcData {
  private val columnIndexMap = schema.fields.map { it -> it.name() to it.pos() }.toMap()

  override fun iterator(): Iterator<JdbcRow> = data.map { it ->
    GenericJdbcRow(it, columnIndexMap)
  }.iterator()

}