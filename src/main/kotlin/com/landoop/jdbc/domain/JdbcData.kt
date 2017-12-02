package com.landoop.jdbc.domain

import org.apache.avro.Schema

interface JdbcData : Iterable<JdbcRow> {
  val schema: Schema

  val table: String?
}