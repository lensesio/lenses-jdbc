package com.landoop.jdbc.domain

interface JdbcRow {
  fun getProperty(column: Int): Any?

  fun getProperty(column: String): Any?
}