@file:Suppress("MayBeConstant")

package io.lenses.jdbc4

object Constants {
  val ProductName = "LSQL for Apache Kafka"
  val DriverName = "JDBC Driver for LSQL"
  val JdbcPrefix = "jdbc:lsql:kafka:"
  val DatabaseName = "default"
  val LensesTokenHeader = "X-Kafka-Lenses-Token"
  val BATCH_HARD_LIMIT = 1000
}