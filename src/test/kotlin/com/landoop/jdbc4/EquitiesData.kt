package com.landoop.jdbc4

import org.apache.avro.LogicalTypes
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.math.BigDecimal
import java.nio.ByteBuffer

interface EquitiesData : ProducerSetup, Logging {

  data class Equity(val ticker: String,
                    val name: String,
                    val price: BigDecimal,
                    val float: Int,
                    val sector: String,
                    val yield: Double?)

  fun populateEquities(): String {

    val topic = createTopic()
    val equities = listOf(
        Equity("goog", "Alphabet", BigDecimal(99.11), 12455235, "Tech", 2.3),
        Equity("bpop", "Banco Popular", BigDecimal(15.34), 5634643, "Financials", 4.4),
        Equity("aapl", "Apple", BigDecimal(13.03), 82346, "Tech", null)
    )

    val foo = SchemaBuilder.fixed("foo").size(8)
    val amount = SchemaBuilder.builder().bytesType()
    val ticker = SchemaBuilder.fixed("ticker").size(4)
    LogicalTypes.decimal(10, 4).addToSchema(amount)

    val schema = SchemaBuilder.record("equity").fields()
        .name("ticker").type(ticker).noDefault()
        .requiredString("name")
        .name("price").type(amount).noDefault()
        .requiredInt("float")
        .requiredString("sector")
        .optionalDouble("yield")
        .endRecord()

    val producer = KafkaProducer<String, GenericData.Record>(producerProps())
    for (equity in equities) {
      logger.debug("Populating with $equity")

      val fixed = GenericData.Fixed(foo)
      fixed.bytes(byteArrayOf(1, 2, 3, 4, 5))

      val record = GenericData.Record(schema)
      record.put("ticker", GenericData.Fixed(ticker, equity.ticker.toByteArray()))
      record.put("name", equity.name)
      record.put("price", ByteBuffer.wrap(equity.price.unscaledValue().toByteArray()))
      record.put("float", equity.float)
      record.put("sector", equity.sector)
      record.put("yield", equity.yield)
      producer.send(ProducerRecord<String, GenericData.Record>(topic, record))
    }
    logger.debug("Population of equities completed")

    return topic
  }
}