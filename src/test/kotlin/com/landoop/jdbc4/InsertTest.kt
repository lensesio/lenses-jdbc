package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
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
    }
  }
}