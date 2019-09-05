package io.lenses.jdbc4.data

import io.kotlintest.matchers.collections.shouldHaveSize
import io.lenses.jdbc4.ProducerSetup
import io.lenses.jdbc4.resultset.toList
import io.lenses.jdbc4.util.Logging
import org.apache.kafka.common.config.TopicConfig
import java.sql.Connection

interface MovieData : ProducerSetup, Logging {

  fun populateMovies(conn: Connection, compacted: Boolean = true): String {

    val topic = createTopic(newTopicName(), if (compacted) TopicConfig.CLEANUP_POLICY_COMPACT else TopicConfig.CLEANUP_POLICY_DELETE)
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
        properties(compacted=$compacted);""".trimIndent()
    ).toList().shouldHaveSize(1)

    conn.createStatement().executeUpdate("""
        insert into $topic(_key, name, year, director, imdb.url, imdb.ranking, imdb.rating)
        VALUES
        ("Shawshank Redemption","Shawshank Redemption", 1998, "Frank Darabont", "http://www.imdb.com/title/tt0111161", 144, 9.2),
        ("The Good, The Bad and the Ugly","The Good, The Bad and the Ugly", 1968, "Sergio Leone","", 1, 8.8),
        ("Interstellar","Interstellar", 2017, "Chris Nolan", "http://www.imdb.com/title/tt0816692", 30, 8.5)
      """.trimIndent())

    return topic
  }
}