package com.landoop.jdbc4

import io.kotlintest.matchers.shouldBe
import io.kotlintest.matchers.shouldThrow
import io.kotlintest.specs.WordSpec
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import java.sql.DriverManager
import java.util.*
import java.util.concurrent.TimeUnit

class InsertTest : WordSpec(), ProducerSetup, Logging {

  private val topic = "topic_" + UUID.randomUUID().toString().replace('-', '_')

  data class Imdb(val url: String, val ranking: Int, val rating: Double)
  data class Movie(val name: String, val year: Int, val director: String, val imdb: Imdb)

  private fun populateMovies() {

    createAdmin().run {
      logger.debug("Creating topic $topic")
      val result = this.createTopics(listOf(NewTopic(topic, 1, 1)))
      logger.debug("Waiting on result")
      result.all().get(10, TimeUnit.SECONDS)
      logger.debug("Closing admin client")
      this.close(10, TimeUnit.SECONDS)
    }

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

    val movies = listOf(
        Movie("Shawshank Redemption", 2017, "Frank Darabont", Imdb("http://www.imdb.com/title/tt0111161", 144, 9.2)),
        Movie("The Good, The Bad and the Ugly", 2017, "Sergio Leone", Imdb("", 1, 8.8)),
        Movie("Interstellar", 2017, "Chris Nolan", Imdb("http://www.imdb.com/title/tt0816692", 30, 8.5))
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

    logger.debug("Closing produder")
    producer.close()
  }

  init {

    LsqlDriver()
    populateMovies()

    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")
    //val conn = DriverManager.getConnection("jdbc:lsql:kafka:https://master.lensesui.dev.landoop.com", "read", "read1")

    "JDBC Driver" should {
      "support insertion" {
        val sql = "INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values ('sammy', '123123123123', 'GBP' ,'smith', 'UK', true)"
        val stmt = conn.createStatement()
        stmt.execute(sql) shouldBe true
      }
      "support prepared statements" {
        val sql = "INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values (?,?,?,?,?,?)"
        val stmt = conn.prepareStatement(sql)
        stmt.setString(1, "sammy")
        stmt.setString(2, "4191005000501123")
        stmt.setString(3, "GBP")
        stmt.setString(4, "smith")
        stmt.setString(5, "UK")
        stmt.setBoolean(6, false)
        stmt.execute() shouldBe true
      }
      "support nested parameters" {
        val sql = "INSERT INTO elements (name, atomic.number, atomic.weight) values (?,?,?)"
        val stmt = conn.prepareStatement(sql)
        stmt.setString(1, "Neodymium")
        stmt.setInt(2, 60)
        stmt.setInt(3, 120)
        stmt.execute() shouldBe true
      }
      "throw an exception trying to set a parameter out of range" {
        val sql = "INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values (?,?,?,?,?,?)"
        val stmt = conn.prepareStatement(sql)
        shouldThrow<IndexOutOfBoundsException> {
          stmt.setString(0, "wibble")
        }
        shouldThrow<IndexOutOfBoundsException> {
          stmt.setString(7, "wibble")
        }
      }
      "return parameter info for prepared statements" {
        val sql = "INSERT INTO cc_data (customerFirstName, number, currency, customerLastName, country, blocked) values (?,?,?,?,?,?)"
        val stmt = conn.prepareStatement(sql)
        val meta = stmt.parameterMetaData
        // this should be 3 even though the schema will have 6
        meta.parameterCount shouldBe 3
        meta.getParameterClassName(1) shouldBe "java.lang.String"
        meta.getParameterClassName(2) shouldBe "java.lang.String"
        meta.getParameterClassName(3) shouldBe "java.lang.String"
      }
    }
  }
}