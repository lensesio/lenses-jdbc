package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldThrow
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager

class InsertTest : WordSpec(), MovieData {
  init {

    LsqlDriver()
    populateMovies()

    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
    //val conn = DriverManager.getConnection("jdbc:lsql:kafka:https://master.lensesui.dev.landoop.com", "read", "read1")

    "JDBC Driver" should {
      "support insertion" {
        val sql = "INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values ('sammy', '123123123123', 'GBP' ,'smith', 'UK', true)"
        val stmt = conn.createStatement()
        stmt.execute(sql) shouldBe true
      }
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
      "return parameter info for prepared statements" {
        val sql = "INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values (?,?,?,?,?,?)"
        val stmt = conn.prepareStatement(sql)
        val meta = stmt.parameterMetaData
        // this should be 3 even though the schema will have 6
        meta.parameterCount shouldBe 3
        meta.getParameterClassName(1) shouldBe "java.lang.String"
        meta.getParameterClassName(2) shouldBe "java.lang.String"
        meta.getParameterClassName(3) shouldBe "java.lang.String"
      }
    }
  }
}