package com.landoop.jdbc4

import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager

import java.sql.SQLException

class PreparedInsertTest : WordSpec(), MovieData {

  init {

    LsqlDriver()
    //populateMovies()

    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")

    "JDBC Driver" should {
      "support prepared statements" {
        val sql = "INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values (?,?,?,?,?,?)"
        val stmt = conn.prepareStatement(sql)
        stmt.setString(1, "sammy")
        stmt.setString(2, "4191005000501123")
        stmt.setString(3, "GBP")
        stmt.setString(4, "smith")
        stmt.setString(5, "UK")
        stmt.setBoolean(6, false)
        stmt.execute() shouldBe true
      }
      "support nested parameters" {
        val sql = "INSERT INTO elements (name, atomic.number, atomic.weight) values (?,?,?)"
        val stmt = conn.prepareStatement(sql)
        stmt.setString(1, "Neodymium")
        stmt.setInt(2, 60)
        stmt.setInt(3, 120)
        stmt.execute() shouldBe true
      }
      "throw an exception trying to set a parameter out of range" {
        val sql = "INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values (?,?,?,?,?,?)"
        val stmt = conn.prepareStatement(sql)
        shouldThrow<IndexOutOfBoundsException> {
          stmt.setString(0, "wibble")
        }
        shouldThrow<IndexOutOfBoundsException> {
          stmt.setString(7, "wibble")
        }
      }
      "throw an exception if a parameter is not set" {
        val sql = "INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values (?,?,?,?,?,?)"
        val stmt = conn.prepareStatement(sql)
        shouldThrow<SQLException> {
          stmt.execute()
        }
      }
      "throw an exception if a nested parameter is not set" {

      }
      "return parameter info for prepared statements" {
        val sql = "INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values (?,?,?,?,?,?)"
        val stmt = conn.prepareStatement(sql)
        val meta = stmt.parameterMetaData
        meta.parameterCount shouldBe 6
        meta.getParameterClassName(1) shouldBe "java.lang.String"
        meta.getParameterClassName(2) shouldBe "java.lang.String"
        meta.getParameterClassName(3) shouldBe "java.lang.String"
        meta.getParameterClassName(4) shouldBe "java.lang.String"
        meta.getParameterClassName(5) shouldBe "java.lang.String"
        meta.getParameterClassName(6) shouldBe "java.lang.Boolean"
      }
    }
  }
}