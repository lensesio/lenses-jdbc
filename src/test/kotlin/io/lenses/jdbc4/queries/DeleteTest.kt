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

      val topic = newTopicName()
      conn.createStatement().executeQuery("""
        create TABLE if not EXISTS 
        $topic(
            _key string,
            name string,year int,
            director string, 
            imdb.url string,
            imdb.ranking int,
            imdb.rating double) 
        format (string, avro)
        properties(compacted=true);""".trimIndent()
      ).toList().shouldHaveSize(1)

      conn.createStatement().executeUpdate("""
        insert into $topic(_key, name, year, director, imdb.url, imdb.ranking, imdb.rating)
        VALUES
        ("Shawshank Redemption","Shawshank Redemption", 1998, "Frank Darabont", "http://www.imdb.com/title/tt0111161", 144, 9.2),
        ("The Good, The Bad and the Ugly","The Good, The Bad and the Ugly", 1968, "Sergio Leone","", 1, 8.8),
        ("Interstellar","Interstellar", 2017, "Chris Nolan", "http://www.imdb.com/title/tt0816692", 30, 8.5)
      """.trimIndent())
      conn.createStatement().executeQuery("SELECT * FROM $topic").toList().shouldHaveSize(3)
      conn.createStatement().executeUpdate("DELETE FROM $topic WHERE name = 'Interstellar'")
      // takes a few seconds to kick in on kafka
      eventually(Duration.ofSeconds(5), AssertionError::class.java) {
        val result = conn.createStatement().executeQuery("SELECT * FROM $topic").toList()
        result.shouldHaveSize(2)
      }
    }

    test("DELETE from table using _value") {

      val topic = populateMovies()
      conn.createStatement().executeQuery("SELECT * FROM $topic").toList().shouldHaveSize(2)
      conn.createStatement().executeUpdate("DELETE FROM $topic WHERE _value.year = 1968")
      // takes a few seconds to kick in on kafka
      eventually(Duration.ofSeconds(5), AssertionError::class.java) {
        val result = conn.createStatement().executeQuery("SELECT * FROM $topic").toList()
        result.shouldHaveSize(1)
      }
    }
  }
}