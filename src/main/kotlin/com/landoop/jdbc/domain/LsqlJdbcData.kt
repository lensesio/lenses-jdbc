package com.landoop.jdbc.domain

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.sql.SQLException


class LsqlJdbcData(private val jdbcRowIterator: Iterator<JdbcRow>,
                   override val schema: Schema,
                   override val table: String?) : JdbcData {
  override fun iterator(): Iterator<JdbcRow> = jdbcRowIterator

  companion object {
    fun from(data: LsqlData): LsqlJdbcData {
      val schema = {
        if (data.schema == null) {
          SchemaBuilder
              .record("lsqldata")
              .fields().nullableString("value", null)
              .endRecord()

        } else {
          try {
            Schema.Parser().parse(data.schema)
          } catch (ex: Exception) {
            throw SQLException("Invalid data schema received.")
          }

        }
      }
      return LsqlJdbcData(schema(), data.topic)
    }
  }
}