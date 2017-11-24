package com.landoop.jdbc

import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types

class LsqlJdbcArray :java.sql.Array{
  private val values: Collection<Any>

  constructor(values: Collection<Any>){
    this.values = values
  }

  @Throws(SQLException::class)
  override fun getBaseTypeName(): String {
    return String::class.java.typeName
  }

  @Throws(SQLException::class)
  override fun getBaseType(): Int {
    return Types.VARCHAR
  }

  @Throws(SQLException::class)
  override fun getArray(): Any {

    return values.toTypedArray()
  }

  @Throws(SQLException::class)
  override fun getArray(map: Map<String, Class<*>>): Any? {
    return null
  }

  @Throws(SQLException::class)
  override fun getArray(index: Long, count: Int): Any? {
    return null
  }

  @Throws(SQLException::class)
  override fun getArray(index: Long, count: Int, map: Map<String, Class<*>>): Any? {
    return null
  }

  @Throws(SQLException::class)
  override fun getResultSet(): ResultSet? {
    return null
  }

  @Throws(SQLException::class)
  override fun getResultSet(map: Map<String, Class<*>>): ResultSet? {
    return null
  }

  @Throws(SQLException::class)
  override fun getResultSet(index: Long, count: Int): ResultSet? {
    return null
  }

  @Throws(SQLException::class)
  override fun getResultSet(index: Long, count: Int, map: Map<String, Class<*>>): ResultSet? {
    return null
  }

  @Throws(SQLException::class)
  override fun free() {

  }
}