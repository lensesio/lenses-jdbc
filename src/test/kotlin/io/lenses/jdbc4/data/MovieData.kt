package io.lenses.jdbc4.data

import io.lenses.jdbc4.ProducerSetup
import io.lenses.jdbc4.util.Logging
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.TopicConfig

interface MovieData : ProducerSetup, Logging {

  data class Imdb(val url: String, val ranking: Int, val rating: Double)
  data class Movie(val name: String, val year: Int, val director: String, val imdb: Imdb)

  fun populateMovies(compactMode: String = TopicConfig.CLEANUP_POLICY_COMPACT): String {

    val topic = createTopic(newTopicName(), compactMode)

    val imdbSchema = SchemaBuilder.record("imdb").fields()
        .requiredString("url")
        .requiredInt("ranking")
        .requiredDouble("rating")
        .endRecord()

    val schema: Schema = SchemaBuilder.record("movie").fields()
        .requiredString("name")
        .requiredInt("year")
        .requiredString("director")
        .name("imdb").type(imdbSchema).noDefault()
        .endRecord()

    schemaClient().register(topic, schema)

    val movies = listOf(
        Movie("Shawshank Redemption",
            1998,
            "Frank Darabont",
            Imdb("http://www.imdb.com/title/tt0111161", 144, 9.2)),
        Movie("The Good, The Bad and the Ugly",
            1968,
            "Sergio Leone",
            Imdb("", 1, 8.8)),
        Movie("Interstellar",
            2017,
            "Chris Nolan",
            Imdb("http://www.imdb.com/title/tt0816692", 30, 8.5))
    )

    val producer = super.createProducer()
    for (movie in movies) {

      val imdb = GenericData.Record(imdbSchema)
      imdb.put("url", movie.imdb.url)
      imdb.put("rating", movie.imdb.rating)
      imdb.put("ranking", movie.imdb.ranking)

      val record = GenericData.Record(schema)
      record.put("name", movie.name)
      record.put("year", movie.year)
      record.put("director", movie.director)
      record.put("imdb", imdb)

      logger.info("Populating movie $record")
      producer.send(ProducerRecord<String, GenericData.Record>(topic, movie.name, record))
    }

    logger.debug("Closing producer")
    producer.close()

    return topic
  }
}