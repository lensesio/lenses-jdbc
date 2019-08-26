package io.lenses.jdbc4.queries

import arrow.core.Try
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.LDriver
import io.lenses.jdbc4.ProducerSetup
import io.lenses.jdbc4.resultset.toList

class ViewTest : FunSpec(), ProducerSetup {
  init {

    LDriver()
    val conn = conn()

    test("VIEW test") {
      Try { conn.createStatement().executeUpdate("DROP VIEW foo") }
      conn.createStatement().executeUpdate("CREATE VIEW foo AS SELECT tpep_pickup_datetime, VendorID FROM nyc_yellow_taxi_trip_data limit 10")
      val rs = conn.createStatement().executeQuery("SELECT * FROM foo")
      rs.metaData.columnCount shouldBe 2
      List(2) { rs.metaData.getColumnLabel(it + 1) } shouldBe listOf("tpep_pickup_datetime", "VendorID")

      Try { conn.createStatement().executeUpdate("DROP VIEW foo") }
      conn.createStatement().executeQuery("SELECT * FROM foo").toList().shouldHaveSize(0)
    }
  }
}