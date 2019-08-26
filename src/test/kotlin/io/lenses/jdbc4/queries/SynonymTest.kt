package io.lenses.jdbc4.queries

import arrow.core.Try
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.LDriver
import io.lenses.jdbc4.ProducerSetup
import io.lenses.jdbc4.resultset.toList

class SynonymTest : FunSpec(), ProducerSetup {
  init {

    LDriver()

    val conn = conn()

    test("Synonym test") {

      Try { conn.createStatement().executeUpdate("DROP Synonym foo") }
      conn.createStatement().executeUpdate("CREATE Synonym foo FOR nyc_yellow_taxi_trip_data")

      val rs = conn.createStatement().executeQuery("SELECT tpep_pickup_datetime, VendorID FROM foo limit 10")
      rs.metaData.columnCount shouldBe 2
      List(2) { rs.metaData.getColumnLabel(it + 1) } shouldBe listOf("tpep_pickup_datetime", "VendorID")
      rs.toList().shouldHaveSize(10)

      conn.createStatement().executeUpdate("DROP Synonym foo")
      conn.createStatement().executeQuery("SELECT * FROM foo").toList().shouldHaveSize(0)
    }
  }
}