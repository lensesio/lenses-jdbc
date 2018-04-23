package com.landoop.jdbc4

import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager
import java.util.*

class BatchNestedInsertStressTest : WordSpec(), LocationData {

  override val random: Random = Random()

  init {
    com.landoop.jdbc4.LsqlDriver()

    val conn = DriverManager.getConnection("jdbc:lsql:kafka:http://localhost:3030", "admin", "admin")

    "JDBC Driver" should {
      "support batched prepared statements" {

        val batchSize = 100
        val count = 1000
        val locations: Array<LocationData.Location> = Array(count, { _ -> randomLocation() })
        logger.debug("Generated $count locations")

        val topic = newTopicName()
        registerValueSchema(topic, schema())
        createTopic(topic)


        val sql = "SET _ktype='STRING';INSERT INTO `$topic` (id, address.street, address.number, address.zip,address.state, geo.lat, geo.lon) values (?,?,?,?,?,?,?)"
        val stmt = conn.prepareStatement(sql)

        locations.asList().chunked(batchSize).forEach { batch ->
          for (location in batch) {
            stmt.setLong(1, location.id)
            stmt.setString(2, location.address.street)
            stmt.setInt(3, location.address.number)
            stmt.setInt(4, location.address.zip)
            stmt.setString(5, location.address.street)
            stmt.setDouble(6, location.geo.lat)
            stmt.setDouble(7, location.geo.lon)
            stmt.addBatch()
          }
          val result = stmt.executeBatch()
          logger.debug("Executed batch")
          result.size shouldBe batchSize
          result.toSet() shouldBe setOf(0)
          stmt.clearBatch()
        }

        // now we must check that our values have been inserted
        for (location in arrayOf(locations.first(), locations.last())) {
          val rs = conn.createStatement()
                  .executeQuery("""
                      SELECT id,
                      `address`.street as street,
                      `address`.number as number,
                      `address`.zip as zip ,
                       geo.lat as lat,
                       geo.lon as lon
                    FROM $topic
                    WHERE _ktype='STRING'
                        AND _vtype='AVRO'
                        AND geo.lat=${location.geo.lat}
                        AND geo.lon=${location.geo.lon}
                        AND `address`.`zip`=${location.address.zip}""".trimIndent())
          rs.next()
          rs.getLong("id") shouldBe location.id
          rs.getString("street") shouldBe location.address.street
          rs.getInt("number") shouldBe location.address.number
          rs.getInt("zip") shouldBe location.address.zip

          rs.getDouble("lat") shouldBe location.geo.lat
          rs.getDouble("lon") shouldBe location.geo.lon

        }
      }
    }
  }
}