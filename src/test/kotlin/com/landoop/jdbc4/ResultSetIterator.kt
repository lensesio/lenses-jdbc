package com.landoop.jdbc4

import java.sql.ResultSet

fun resultSetIterator(rs: ResultSet): Iterator<ResultSet> {
  return object : Iterator<ResultSet> {
    override fun next(): ResultSet = rs
    override fun hasNext(): Boolean = rs.next()
  }
}