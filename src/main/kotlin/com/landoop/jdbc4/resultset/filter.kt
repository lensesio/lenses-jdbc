package com.landoop.jdbc4.resultset

import java.sql.ResultSet

fun ResultSet.filter(f: (ResultSet) -> Boolean): ResultSet {
  val outer = this
  return object : ResultSet by this {
    override tailrec fun next(): Boolean {
      if (!outer.next())
        return false
      if (f(this))
        return true
      return next()
    }
  }
}