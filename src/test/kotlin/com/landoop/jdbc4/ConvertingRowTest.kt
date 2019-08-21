package com.landoop.jdbc4

import com.landoop.jdbc4.row.ListRow
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import java.math.BigDecimal
import java.time.Instant
import java.time.ZoneId

class ConvertingRowTest : WordSpec() {
  init {
    "ConvertingRow" should {
      "convert char to String" {
        ListRow(listOf('a')).getString(1) shouldBe "a"
      }
      "convert number to String" {
        ListRow(listOf(1)).getString(1) shouldBe "1"
        ListRow(listOf(1L)).getString(1) shouldBe "1"
        ListRow(listOf(1.1)).getString(1) shouldBe "1.1"
        ListRow(listOf(1.1F)).getString(1) shouldBe "1.1"
      }
      "convert boolean to String" {
        ListRow(listOf(true)).getString(1) shouldBe "true"
      }
      "convert int to long" {
        ListRow(listOf(1)).getLong(1) shouldBe 1L
      }
      "convert String to long" {
        ListRow(listOf("1")).getLong(1) shouldBe 1L
      }
      "convert long to int" {
        ListRow(listOf(1L)).getInt(1) shouldBe 1
      }
      "convert String to int" {
        ListRow(listOf("1")).getInt(1) shouldBe 1
      }
      "convert String to boolean" {
        ListRow(listOf("true")).getBoolean(1) shouldBe true
      }
      "convert String to double" {
        ListRow(listOf("1.1")).getDouble(1) shouldBe 1.1
      }
      "convert String to float" {
        ListRow(listOf("1.1")).getFloat(1) shouldBe 1.1F
      }
      "convert Long to Date" {
        ListRow(listOf(123456789L)).getDate(1).time shouldBe Instant.ofEpochMilli(123456789L).atZone(ZoneId.of("Z")).toInstant().toEpochMilli()
      }
      "convert Int to Date" {
        ListRow(listOf(123)).getDate(1) shouldBe java.sql.Date.from(Instant.ofEpochMilli(123))
      }
      "convert Long to BigDecimal" {
        ListRow(listOf(123L)).getBigDecimal(1) shouldBe BigDecimal(123L)
      }
      "convert int to byte" {
        val b: Byte = 1
        ListRow(listOf("1")).getByte(1) shouldBe b
      }
      "convert String to Reader" {
        val reader = ListRow(listOf("abc")).charStream(1)
        reader.readText() shouldBe "abc"
      }
    }
  }
}