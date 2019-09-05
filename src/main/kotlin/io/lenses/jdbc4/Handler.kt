package io.lenses.jdbc4

import java.sql.ResultSet

interface Handler {
  fun execute(): ResultSet
}