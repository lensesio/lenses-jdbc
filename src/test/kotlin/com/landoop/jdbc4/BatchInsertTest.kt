package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager
import java.util.*

class BatchInsertTest : WordSpec(), MovieData {
  init {

    val countries = listOf("UK", "US", "DE", "ES", "FR")
    val currencies = listOf("GBP", "USD", "AUD", "YEN", "EUR")
    val surnames = listOf("picard", "riker", "troi", "crusher", "yar", "la forge", "son of mogh", "obrien", "soong")
    val firstnames = listOf("jean luc", "william", "deanna", "beverley", "tasha", "geordi", "worf", "wesley", "miles", "data")
    LsqlDriver()

    data class Values(val country: String,
                      val currency: String,
                      val surname: String,
                      val firstname: String,
                      val blocked: Boolean,
                      val number: String)

    fun <T> randomElement(list: List<T>): T = list[Random().nextInt(list.size)]

    fun randomCountry() = randomElement(countries)
    fun randomCurrency() = randomElement(currencies)
    fun randomFirstName() = randomElement(firstnames)
    fun randomSurname() = randomElement(surnames)
    fun randomCardNumber() = IntArray(16, { _ -> Random().nextInt(9) }).joinToString("")

    val values = Array(50, { _ -> Values(randomCountry(), randomCurrency(), randomSurname(), randomFirstName(), Random().nextBoolean(), randomCardNumber()) })

    // val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
    val conn = DriverManager.getConnection("jdbc:lsql:kafka:https://master.lensesui.dev.landoop.com", "write", "write1")

    "JDBC Driver" should {
      "support batched prepared statements" {
        val sql = "INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values (?,?,?,?,?,?)"
        val stmt = conn.prepareStatement(sql)

        for (value in values) {
          stmt.setString(1, value.firstname)
          stmt.setString(2, value.number)
          stmt.setString(3, value.currency)
          stmt.setString(4, value.surname)
          stmt.setString(5, value.country)
          stmt.setBoolean(6, value.blocked)
          stmt.addBatch()
        }

        val result = stmt.executeBatch()
        result.size shouldBe 50
        result.toSet() shouldBe setOf(0)

        // now we must check that our values have been inserted
        for (value in values) {
          val rs = conn.createStatement().executeQuery("SELECT * FROM cc_data WHERE _ktype='STRING' AND _vtype='AVRO' AND customerLastName='${value.surname}' and customerFirstName='${value.firstname}' AND number='${value.number}'")
          rs.next()
          rs.getString("customerFirstName") shouldBe value.firstname
          rs.getString("customerLastName") shouldBe value.surname
          rs.getString("number") shouldBe value.number
          rs.getString("currency") shouldBe value.currency
          rs.getString("country") shouldBe value.country
          rs.getBoolean("blocked") shouldBe value.blocked
        }
      }
    }
  }
}