package io.lenses.jdbc4.queries

import io.kotlintest.eventually
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.data.MovieData
import io.lenses.jdbc4.resultset.toList
import java.time.Duration

class TruncateTest : FunSpec(), MovieData {
  init {

    test("truncate table") {
      val conn = conn()
      val topic = populateMovies(conn, false)

      conn.createStatement().executeQuery("SELECT * FROM `$topic`").toList().shouldHaveSize(3)
      conn.createStatement().executeUpdate("TRUNCATE TABLE `$topic`")
      // use eventually because the topic has to be deleted and recreated which can take a few seconds
      eventually(Duration.ofSeconds(15)) {
        conn.createStatement().executeQuery("SELECT * FROM `$topic`").toList().shouldHaveSize(0)
      }
    }
  }
}