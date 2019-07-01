package com.landoop.jdbc4

import com.landoop.jdbc4.util.Logging
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

interface LocationData : ProducerSetup, Logging {

  val random: Random

  data class Address(val street: String, val number: Int, val zip: Int, val state: String)
  data class Geo(val lat: Double, val lon: Double)
  data class Location(val id: Long, val address: Address, val geo: Geo)

  val states: List<String>
    get() = listOf("Texas", "Utah", "Lousiana", "Hawaii", "New York", "California", "Oregon", "Iowa", "Montana", "Florida", "Georgia", "Maryland", "Oklahoma", "Washington", "Vermont", "Ohio", "Idaho", "Nebraska")

  val streets: List<String>
    get() = listOf("Baker", "Picadilly", "Northern", "Royal", "Oak", "Finchley", "St Johns", "St Pauls", "St Peters", "St Marks", "Fleet", "Hampshire", "Marylebone", "Farringdon")

  val endings: List<String>
    get() = listOf("Road", "Street", "Avenue", "Close", "Drive", "Highway", "Walk", "West", "East")

  fun <T> randomElement(list: List<T>): T = list[random.nextInt(list.size)]

  fun randomGeo(): Geo = Geo(random.nextDouble(), random.nextDouble())
  fun randomZipCode(): Int = random.nextInt(89999) + 10000
  fun randomStreet(): String = randomElement(streets) + " " + randomElement(endings)

  fun randomState() = randomElement(states)
  fun randomAddress(): Address = Address(randomStreet(), random.nextInt(9999), randomZipCode(), randomState())
  fun randomLocation() = Location(random.nextLong(), randomAddress(), randomGeo())

  fun addressSchema(): Schema = SchemaBuilder.record("address").fields()
      .requiredString("street")
      .requiredInt("number")
      .requiredInt("zip")
      .requiredString("state")
      .endRecord()

  fun geoSchema(): Schema = SchemaBuilder.record("geo").fields()
      .requiredDouble("lat")
      .requiredDouble("lon")
      .endRecord()

  fun schema(): Schema = SchemaBuilder.record("location").fields()
      .requiredLong("id")
      .name("address").type(addressSchema()).noDefault()
      .name("geo").type(geoSchema()).noDefault()
      .endRecord()

  fun record(location: Location): GenericData.Record {
    val address = GenericData.Record(addressSchema())
    address.put("street", location.address.street)
    address.put("number", location.address.number)
    address.put("zip", location.address.zip)
    address.put("state", location.address.state)

    val geo = GenericData.Record(geoSchema())
    geo.put("lat", location.geo.lat)
    geo.put("lon", location.geo.lon)

    val record = GenericData.Record(schema())
    record.put("id", random.nextLong())
    record.put("address", address)
    record.put("geo", geo)

    return record
  }

  fun populateLocations(): String {
    val topic = createTopic()
    val locations = List(5, { _ -> randomLocation() })
    registerValueSchema(topic, schema())
    val producer = super.createProducer()

    for (location in locations) {
      val record = record(location)
      logger.info("Populating location $record")
      producer.send(ProducerRecord<String, GenericData.Record>(topic, record.get("id").toString(), record))
    }

    logger.debug("Closing producer")
    producer.close()

    return topic
  }
}