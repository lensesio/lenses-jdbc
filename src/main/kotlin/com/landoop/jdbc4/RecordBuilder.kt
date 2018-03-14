package com.landoop.jdbc4

import com.landoop.rest.domain.InsertField
import com.landoop.rest.domain.PreparedInsertInfo
import java.sql.SQLException

// used by prepared statements to build up records
class RecordBuilder(val info: PreparedInsertInfo) {

  // all values in this list are paths of branch nodes
  private val branches = info.fields.filter { it.parents.isNotEmpty() }.map { it.parents.joinToString(".") }

  private var key: Any? = null
  private val values = mutableMapOf<String, Any?>()

  private fun InsertField.path(): String {
    val parents = this.parents.toMutableList()
    parents.add(this.name)
    return parents.joinToString(".")
  }

  private fun InsertField.isBranch(): Boolean = branches.contains(this.path())
  private fun InsertField.isLeaf(): Boolean = !this.isBranch()

  private fun put(field: InsertField, value: Any?) {
    // we cannot insert a value to a branch, all values must be
    // inserted into leaf nodes
    when {
      field.isKey -> key = value
      field.isBranch() -> throw SQLException("Can only insert values into leaf fields; ${field.path()} is a branch field")
      else -> values[field.name] = value
    }
  }

  // sets a value by name, using the fields info to locate the fields
  fun put(key: String, value: Any?) {
    val field = info.fields.find { it.name == key } ?: throw SQLException("Unknown field $key")
    put(field, value)
  }

  // sets a value by index, using the fields info to locate that field
  fun put(index: Int, value: Any?) {
    checkBounds(index)
    // remember jdbc indexes are 1 based
    val field = info.fields[index - 1]
    put(field, value)
  }

  private fun checkBounds(k: Int) {
    if (k < 1 || k > info.fields.size)
      throw IndexOutOfBoundsException("$k is out of bounds")
  }

  // throws an exception if this record is not valid because of missing values
  fun checkRecord() {

    // if we have a key field, we must set the key
    // todo check this assumption
    if (info.fields.any { it.isKey })
      if (key == null)
        throw SQLException("Key field must be specified")

    info.fields.filterNot { it.isKey }.forEach { field ->
      val path = field.path()
      when {
        field.isBranch() ->
          if (values.containsKey(path))
            throw SQLException("Variable $path was set; You cannot set values on branches")
        field.isLeaf() ->
          if (!values.containsKey(path))
            throw SQLException("Variable $path was not set; You must set all values before executing; if null is desired, explicitly set this using setNull(pos)")
      }
    }
  }
}