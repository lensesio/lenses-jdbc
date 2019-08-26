package io.lenses.jdbc4.queries

import io.kotlintest.eventually
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.LDriver
import io.lenses.jdbc4.data.MovieData
import io.lenses.jdbc4.resultset.toList
import java.time.Duration

class DeleteTest : FunSpec(), MovieData {
  init {

    LDriver()
    val conn = conn()

    test("DELETE from table test") {

      val topic = populateMovies()
      conn.createStatement().executeQuery("SELECT * FROM $topic").toList().shouldHaveSize(3)
      conn.createStatement().executeUpdate("DELETE FROM $topic WHERE name = 'Interstellar'")
      // takes a few seconds to kick in on kafka
      eventually(Duration.ofSeconds(5), AssertionError::class.java) {
        conn.createStatement().executeQuery("SELECT * FROM $topic").toList().shouldHaveSize(2)
      }
    }

    test("DELETE from table using _value") {

      val topic = populateMovies()
      conn.createStatement().executeQuery("SELECT * FROM $topic").toList().shouldHaveSize(2)
      conn.createStatement().executeUpdate("DELETE FROM $topic WHERE _value.year = 1968")
      // takes a few seconds to kick in on kafka
      eventually(Duration.ofSeconds(5), AssertionError::class.java) {
        conn.createStatement().executeQuery("SELECT * FROM $topic").toList().shouldHaveSize(1)
      }
    }
  }
}