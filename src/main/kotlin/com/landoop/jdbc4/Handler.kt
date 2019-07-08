package com.landoop.jdbc4

import java.sql.ResultSet

interface Handler {
  fun execute(): ResultSet
}