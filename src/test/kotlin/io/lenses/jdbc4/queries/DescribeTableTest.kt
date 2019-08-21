package io.lenses.jdbc4.queries

import io.kotlintest.matchers.collections.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.LDriver
import io.lenses.jdbc4.resultset.toList
import java.sql.DriverManager

class DescribeTableTest : FunSpec() {
  init {

    LDriver()

    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:24015", "admin", "admin999")

    test("DESCRIBE TABLE foo schema") {
      val q = "DESCRIBE TABLE nyc_yellow_taxi_trip_data"
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(q)
      rs.metaData.columnCount shouldBe 2
      List(2) { rs.metaData.getColumnLabel(it + 1) } shouldBe listOf("key", "value")
    }

    test("DESCRIBE TABLE foo data") {
      val q = "DESCRIBE TABLE nyc_yellow_taxi_trip_data"
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(q).toList()
      rs.shouldContain(listOf("_value.pickup_longitude", "double"))
      rs.shouldContain(listOf("_value.pickup_latitude", "double"))
      rs.shouldContain(listOf("_value.RateCodeID", "int"))
      rs.shouldContain(listOf("Value", "AVRO"))
      rs.shouldContain(listOf("Key", "BYTES"))
    }
  }
}