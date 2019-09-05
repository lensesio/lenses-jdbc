//package io.lenses.jdbc4
//
//import io.kotlintest.shouldBe
//import io.kotlintest.shouldThrow
//import io.kotlintest.specs.WordSpec
//import java.sql.DriverManager
//import java.sql.SQLException
//
//class BatchInsertTest : WordSpec(), CCData {
//  init {
//    io.lenses.jdbc4.LDriver()
//
//    val conn = DriverManager.getConnection("jdbc:lenses:kafka:http://localhost:3030", "admin", "admin")
//
//    "JDBC Driver" should {
//      "support batched prepared statements" {
//
//        val batchSize = 20
//        val values = Array(batchSize, { _ -> generateCC() })
//        val sql = "SET _ktype='STRING'; SET _vtype='AVRO';INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values (?,?,?,?,?,?)"
//        val stmt = conn.prepareStatement(sql)
//
//        for (value in values) {
//          stmt.setString(1, value.firstname)
//          stmt.setString(2, value.number)
//          stmt.setString(3, value.currency)
//          stmt.setString(4, value.surname)
//          stmt.setString(5, value.country)
//          stmt.setBoolean(6, value.blocked)
//          stmt.addBatch()
//        }
//
//        val result = stmt.executeBatch()
//        result.size shouldBe batchSize
//        result.toSet() shouldBe setOf(0)
//        stmt.clearBatch()
//
//        // now we must check that our values have been inserted
//        for (value in values) {
//          val rs = conn.createStatement().executeQuery("SELECT * FROM cc_data WHERE _ktype='STRING' AND _vtype='AVRO' AND customerLastName='${value.surname}' and customerFirstName='${value.firstname}' AND number='${value.number}'")
//          rs.next()
//          rs.getString("customerFirstName") shouldBe value.firstname
//          rs.getString("customerLastName") shouldBe value.surname
//          rs.getString("number") shouldBe value.number
//          rs.getString("currency") shouldBe value.currency
//          rs.getString("country") shouldBe value.country
//          rs.getBoolean("blocked") shouldBe value.blocked
//        }
//      }
//      "throw exception if batch size exceeded" {
//        val sql = "INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values (?,?,?,?,?,?)"
//        val stmt = conn.prepareStatement(sql)
//        fun add() {
//          stmt.setString(1, "a")
//          stmt.setString(2, "123")
//          stmt.setString(3, "GBP")
//          stmt.setString(4, "b")
//          stmt.setString(5, "UK")
//          stmt.setBoolean(6, false)
//          stmt.addBatch()
//        }
//        for (k in 1..io.lenses.jdbc4.Constants.BATCH_HARD_LIMIT) {
//          add()
//        }
//        // the next element should exceed the batch limit and throw an exception
//        shouldThrow<SQLException> {
//          add()
//        }
//      }
//    }
//  }
//}