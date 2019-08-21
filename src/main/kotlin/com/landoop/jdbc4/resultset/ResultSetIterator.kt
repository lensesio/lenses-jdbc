package com.landoop.jdbc4.resultset

import java.sql.ResultSet

fun resultSetIterator(rs: ResultSet): Iterator<ResultSet> {
  return object : Iterator<ResultSet> {
    override fun next(): ResultSet = rs
    override fun hasNext(): Boolean = rs.next()
  }
}

fun resultSetList(rs: ResultSet): List<List<Any?>> {
  val results = mutableListOf<List<Any?>>()
  while (rs.next()) {
    val row = (1..rs.metaData.columnCount).map { rs.getObject(it) }
    results.add(row.toList())
  }
  return results.toList()
}