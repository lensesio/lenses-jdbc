package io.lenses.jdbc4

import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager

class InsertTest : WordSpec(), CCData {
  init {

    io.lenses.jdbc4.LDriver()

    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")

    "JDBC Driver" should {
      "support insertion"{
        try {
          val card = generateCC()
          val sql = "INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values ('${card.firstname}', ${card.number}, '${card.currency}' ,'${card.surname}', '${card.country}', ${card.blocked})"
          val stmt = conn.createStatement()
          stmt.execute(sql) shouldBe true

          // now we must check that our values have been inserted
          val rs = conn.createStatement().executeQuery("SELECT * FROM cc_data WHERE _ktype='STRING' AND _vtype='AVRO' AND customerLastName='${card.surname}' and customerFirstName='${card.firstname}' AND number='${card.number}'")
          rs.next()
          rs.getString("customerFirstName") shouldBe card.firstname
          rs.getString("customerLastName") shouldBe card.surname
          rs.getString("number") shouldBe card.number
          rs.getString("currency") shouldBe card.currency
          rs.getString("country") shouldBe card.country
          rs.getBoolean("blocked") shouldBe card.blocked
        } catch (t: Throwable) {
          t.printStackTrace()
          throw t
        }
      }
      "support insertion mixed case" {
        try {
          val card = generateCC()
          val sql = "inSerT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values ('${card.firstname}', ${card.number}, '${card.currency}' ,'${card.surname}', '${card.country}', ${card.blocked})"
          val stmt = conn.createStatement()
          stmt.execute(sql) shouldBe true

          // now we must check that our values have been inserted
          val rs = conn.createStatement().executeQuery("SET `max.zero.polls` = 20;SELECT * FROM cc_data WHERE _ktype='STRING' AND _vtype='AVRO' AND customerLastName='${card.surname}' and customerFirstName='${card.firstname}' AND number='${card.number}'")
          rs.next()
          rs.getString("customerFirstName") shouldBe card.firstname
          rs.getString("customerLastName") shouldBe card.surname
          rs.getString("number") shouldBe card.number
          rs.getString("currency") shouldBe card.currency
          rs.getString("country") shouldBe card.country
          rs.getBoolean("blocked") shouldBe card.blocked
        } catch (t: Throwable) {
          t.printStackTrace()
          throw t
        }
      }
    }
  }
}