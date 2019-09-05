package io.lenses.jdbc4.queries

import io.kotlintest.eventually
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.LensesDriver
import io.lenses.jdbc4.data.MovieData
import io.lenses.jdbc4.resultset.toList
import org.apache.kafka.common.config.TopicConfig
import java.time.Duration

class DeleteTest : FunSpec(), MovieData {
  init {

    LensesDriver()
    val conn = conn()

    test("DELETE from table test") {

      val topic = populateMovies(conn)

      conn.createStatement().executeQuery("SELECT * FROM $topic").toList().shouldHaveSize(3)
      conn.createStatement().executeUpdate("DELETE FROM $topic WHERE name = 'Interstellar'")
      // takes a few seconds to kick in on kafka
      eventually(Duration.ofSeconds(5), AssertionError::class.java) {
        val result = conn.createStatement().executeQuery("SELECT * FROM $topic").toList()
        // kafka will insert a new record with the key and value == null
        result.shouldHaveSize(4)
      }
    }

    test("DELETE from table using _value") {

      val topic = populateMovies(conn)
      conn.createStatement().executeUpdate("DELETE FROM $topic WHERE _value.year = 1968")
      // takes a few seconds to kick in on kafka
      eventually(Duration.ofSeconds(5), AssertionError::class.java) {
        val result = conn.createStatement().executeQuery("SELECT * FROM $topic").toList()
        // kafka will insert a new record with the key and value == null
        result.shouldHaveSize(4)
      }
    }
  }
}