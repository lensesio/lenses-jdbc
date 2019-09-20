package io.lenses.jdbc4.queries

import io.kotlintest.matchers.collections.shouldBeEmpty
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import io.lenses.jdbc4.LensesDriver
import io.lenses.jdbc4.ProducerSetup
import io.lenses.jdbc4.resultset.toList
import java.sql.DriverManager

class PollTopicViaJdbcTest : WordSpec(), ProducerSetup {

  init {

    LensesDriver()

    val conn = conn()


    "JDBC Driver" should {
      "read a topic via polling" {
        val topic = newTopicName()
        conn.createStatement().executeQuery("""
          CREATE TABLE $topic(_key string, name string, difficulty int) FORMAT (Avro, Avro);
        """.trimIndent())

        var lastOffset = 0
        val sql1 = "SELECT * FROM $topic WHERE _meta.partition=0 and _meta.offset >= $lastOffset"
        conn.createStatement().executeQuery(sql1).toList().shouldHaveSize(0)


        conn.createStatement().executeQuery("""
          INSERT INTO $topic(_key, name, difficulty) 
          VALUES("1", "Learn Lenses SQL", 3),
          ("2", "Learn Quantum Physics", 10);
        """.trimIndent())

        val sql2 = "SELECT *, _meta.offset as _offset, _key as key FROM $topic WHERE _meta.partition=0 and _meta.offset >= $lastOffset"
        val rs = conn.createStatement().executeQuery(sql2)
        val list = mutableListOf<Any>()
        while (rs.next()) {
          val key = rs.getString("key")
          val name = rs.getString("name")
          val difficulty = rs.getInt("difficulty")
          val offset = rs.getInt("_offset")
          list.add(listOf(key, name, difficulty, offset))
        }
        list.shouldHaveSize(2)
        list shouldBe listOf(
            listOf("1", "Learn Lenses SQL", 3, 0),
            listOf("2", "Learn Quantum Physics", 10, 1)
        )
        lastOffset = 1

        val sql3 = "SELECT *, _meta.offset as _offset, _key as key FROM $topic WHERE _meta.partition=0 and _meta.offset > $lastOffset"
        conn.createStatement().executeQuery(sql3).toList().shouldBeEmpty()
        conn.createStatement().executeQuery("""
          INSERT INTO $topic(_key, name, difficulty) 
          VALUES("3", "Learn French", 5);
        """.trimIndent())

        val sql4 = "SELECT *, _meta.offset as _offset, _key as key FROM $topic WHERE _meta.partition=0 and _meta.offset > $lastOffset"
        val rs2 = conn.createStatement().executeQuery(sql4)
        val oneRecordList = mutableListOf<Any>()
        while (rs2.next()) {
          val key = rs2.getString("key")
          val name = rs2.getString("name")
          val difficulty = rs2.getInt("difficulty")
          val offset = rs2.getInt("_offset")
          oneRecordList.add(listOf(key, name, difficulty, offset))
        }
        oneRecordList.shouldHaveSize(1)
        oneRecordList shouldBe listOf(
            listOf(
                "3", "Learn French", 5, 2
            )
        )
        lastOffset = 2
        val sql5 = "SELECT *, _meta.offset as _offset, _key as key FROM $topic WHERE _meta.partition=0 and _meta.offset > $lastOffset"
        conn.createStatement().executeQuery(sql5).toList().shouldBeEmpty()
      }
    }
  }
}