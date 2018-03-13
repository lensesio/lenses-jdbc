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
        Equity("ge", "General Electric", BigDecimal(15.34), 5634643, "Industrials", 4.4),
        Equity("dbk", "Deutsche Bank", BigDecimal(13.03), 82346, "Financials", null)
    )

    val foo = SchemaBuilder.fixed("foo").size(5)
    val amount = SchemaBuilder.builder().bytesType()
    LogicalTypes.decimal(10, 4).addToSchema(amount)

    val schema = SchemaBuilder.record("equity").fields()
        .requiredString("ticker")
        .requiredString("name")
        .name("price").type(amount).noDefault()
        .requiredInt("float")
        .requiredString("sector")
        .optionalDouble("yield")
        .endRecord()

    val producer = KafkaProducer<String, GenericData.Record>(producerProps())
    for (equity in equities) {

      val fixed = GenericData.Fixed(foo)
      fixed.bytes(byteArrayOf(1, 2, 3, 4, 5))

      val record = GenericData.Record(schema)
      record.put("ticker", equity.ticker)
      record.put("name", equity.name)
      record.put("price", ByteBuffer.wrap(equity.price.unscaledValue().toByteArray()))
      record.put("float", equity.float)
      record.put("sector", equity.sector)
      record.put("yield", equity.yield)
      producer.send(ProducerRecord<String, GenericData.Record>(topic, equity.ticker, record))
    }

    return topic
  }
}