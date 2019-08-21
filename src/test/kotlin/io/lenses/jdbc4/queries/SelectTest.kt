package io.lenses.jdbc4.queries

import io.kotlintest.assertSoftly
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.WordSpec
import io.lenses.jdbc4.LDriver
import io.lenses.jdbc4.ProducerSetup
import java.sql.DriverManager
import java.sql.SQLException

class SelectTest : WordSpec(), ProducerSetup {

  init {

    LDriver()

    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:24015", "admin", "admin999")

    "JDBC Driver" should {
      "throw error for unknown table" {
        shouldThrow<SQLException> {
          val q = "SELECT * FROM `qweqweqwe` WHERE _vtype='AVRO' AND _ktype='STRING'"
          val stmt = conn.createStatement()
          stmt.executeQuery(q)
        }
      }
      "support wildcard selection" {
        val q = "SELECT * FROM `nyc_yellow_taxi_trip_data`"
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 19
        List(19) { rs.metaData.getColumnLabel(it + 1) }.toSet() shouldBe
            setOf("VendorID",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "passenger_count",
                "trip_distance",
                "pickup_longitude",
                "pickup_latitude",
                "RateCodeID",
                "store_and_fwd_flag",
                "dropoff_longitude",
                "dropoff_latitude",
                "payment_type",
                "fare_amount",
                "extra",
                "mta_tax",
                "improvement_surcharge",
                "tip_amount",
                "tolls_amount",
                "total_amount")
      }
      "support projections" {
        val q = "SELECT trip_distance, creditCardId FROM `nyc_yellow_taxi_trip_data`"
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 2
        assertSoftly {
          rs.metaData.getColumnLabel(1) shouldBe "trip_distance"
          rs.metaData.getColumnLabel(2) shouldBe "creditCardId"
        }
      }
      "support projections with backticks" {
        val q = "SELECT `trip_distance`, `creditCardId` FROM `nyc_yellow_taxi_trip_data`"
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 2
        assertSoftly {
          rs.metaData.getColumnLabel(1) shouldBe "trip_distance"
          rs.metaData.getColumnLabel(2) shouldBe "creditCardId"
        }
      }
      "support queries with white space" {
        val q = "SELECT         `trip_distance`, `creditCardId`         FROM `nyc_yellow_taxi_trip_data`"
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 2
        assertSoftly {
          rs.metaData.getColumnLabel(1) shouldBe "trip_distance"
          rs.metaData.getColumnLabel(2) shouldBe "creditCardId"
        }
      }
      "support queries with new lines" {
        val q = "SELECT         `trip_distance`, \n" +
            "`creditCardId`       \n" +
            "  FROM `nyc_yellow_taxi_trip_data`"
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 2
        assertSoftly {
          rs.metaData.getColumnLabel(1) shouldBe "trip_distance"
          rs.metaData.getColumnLabel(2) shouldBe "creditCardId"
        }
      }
      "support limits"  {
        val q = "SELECT trip_distance, creditCardId FROM `nyc_yellow_taxi_trip_data` limit 43"
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 2
        assertSoftly {
          rs.metaData.getColumnLabel(1) shouldBe "trip_distance"
          rs.metaData.getColumnLabel(2) shouldBe "creditCardId"
        }
        var counter = 0
        while (rs.next()) {
          counter += 1
        }
        counter shouldBe 43
      }
      "support where" {
        val q = "SELECT trip_distance FROM `nyc_yellow_taxi_trip_data` where trip_distance > 2"
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 1
        assertSoftly {
          rs.metaData.getColumnLabel(1) shouldBe "trip_distance"
        }
      }
      "support where with backticks" {
        val q = "SELECT `trip_distance` FROM `nyc_yellow_taxi_trip_data` where `trip_distance` > 2"
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(q)
        rs.metaData.columnCount shouldBe 1
        assertSoftly {
          rs.metaData.getColumnLabel(1) shouldBe "trip_distance"
        }
      }
      "return true for valid query" {
        conn.createStatement().execute("SELECT trip_distance, creditCardId FROM `nyc_yellow_taxi_trip_data` limit 43") shouldBe true
      }
    }
  }
}