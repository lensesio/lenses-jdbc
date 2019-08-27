package io.lenses.jdbc4.queries

import arrow.core.Try
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.FunSpec
import io.lenses.jdbc4.LensesDriver
import io.lenses.jdbc4.ProducerSetup
import io.lenses.jdbc4.resultset.toList
import java.sql.Connection
import java.sql.SQLException

class SynonymTest : FunSpec(), ProducerSetup {
  init {

    LensesDriver()

    val conn = conn()

    test("Synonym test") {
      val topic = createTopic(conn)
      createTopicData(conn, topic)
      val synonymName = "mysynonim" + System.currentTimeMillis()
      Try { conn.createStatement().executeUpdate("DROP Synonym $synonymName") }
      conn.createStatement().executeUpdate("CREATE Synonym $synonymName FOR $topic")

      val rs = conn.createStatement().executeQuery("SELECT tpep_pickup_datetime, VendorID FROM $synonymName limit 10")
      rs.metaData.columnCount shouldBe 2
      List(2) { rs.metaData.getColumnLabel(it + 1) } shouldBe listOf("tpep_pickup_datetime", "VendorID")
      rs.toList().shouldHaveSize(10)

      conn.createStatement().executeUpdate("DROP Synonym $synonymName")
      shouldThrow<SQLException> {
        conn.createStatement().executeQuery("SELECT * FROM $synonymName")
      }
    }
  }

  fun createTopic(conn: Connection): String {
    val topic = newTopicName()
    conn.createStatement().executeQuery("""
            create table $topic(
              _key string,
              VendorID int,
              tpep_pickup_datetime string,
              tpep_dropoff_datetime string,
              passenger_count int,
              trip_distance double, 
              pickup_longitude double,
              pickup_latitude double,
              RateCodeID int,
              store_and_fwd_flag string, 
              dropoff_longitude double,
              dropoff_latitude double,
              payment_type int,
              fare_amount double,
              extra double,
              mta_tax double,
              improvement_surcharge double,
              tip_amount double,
              tolls_amount double,
              total_amount double)
              format(string, avro)
        """.trimIndent()).toList().shouldHaveSize(1)
    return topic
  }

  private fun createTopicData(conn: Connection, topic: String) {
    conn.createStatement()
        .executeQuery("""
       insert into $topic(
              VendorID,
              tpep_pickup_datetime,
              tpep_dropoff_datetime,
              passenger_count,
              trip_distance, 
              pickup_longitude,
              pickup_latitude,
              RateCodeID,
              store_and_fwd_flag, 
              dropoff_longitude,
              dropoff_latitude,
              payment_type,
              fare_amount,
              extra,
              mta_tax,
              improvement_surcharge,
              tip_amount,
              tolls_amount,
              total_amount)
        VALUES
        (1,'2016-03-20 19:20:09',	'2016-03-20 19:23:25',	1,	0.7 ,	-73.99173736572266, 40.7386589050293,	  1,	'N',	-73.99967956542969,	40.73412322998047,	1,	4.5,	0,	0.5,	0.3,	1, 0,	6.3),
        (1,'2016-03-20 19:20:10', '2016-03-20 19:25:52',	2, 	1.4	, -73.99103546142578,	40.76046371459961,	1,	'N',	-73.98226928710938,	40.77155303955078,  2, 	5.5,	0,	0.5,	0.3,	0, 0,	6.3),
        (2,'2016-03-20 19:20:11',	'2016-03-20 19:27:09',	1,	0.66,	-73.98735046386719,	40.75653076171875,	1,	'N',	-73.9967269897461,	40.76028060913086,  1,	32.5,	0,	0.5,	0.3,	5, 5.54,	43.84),
        (2,'2016-03-20 19:20:13',	'2016-03-20 19:26:47',	1,	1.61,	-73.96199035644531,	40.77945327758789,	1,	'N',	-73.98197937011719,	40.77967071533203,  1,	4.5,	0,	0.5,	0.3,	2, 0,	7.3),
        (1,'2016-03-20 19:20:15',	'2016-03-20 19:25:46',	1,	1.9	,-74.00791931152344,	40.74011993408203,	1,	'N',	-74.0147933959961,	40.715457916259766, 1,	15.5,	0,	0.5,	0.3,	2.5, 0,	18.8),
        (1,'2016-03-20 19:20:17',	'2016-03-20 19:25:14',	1,	1.3	,-73.95572662353516,	40.78500747680664,	1,	'N',	-73.96957397460938,	40.79618453979492,  1,	4.5,	0,	0.5,	0.3,	1.05,	0,	6.35),
        (2,'2016-03-20 19:20:18',	'2016-03-20 19:23:29',	1,	0.55,	-73.99325561523438,	40.72775650024414,	1,	'N',	-73.987060546875,	  40.729312896728516, 1,	14.5,	0,	0.5,	0.3,	3.8,	0,	19.1),
        (2,'2016-03-20 19:20:20',	'2016-03-20 19:43:50',	1,	4.17,	-74.0016860961914,	40.73445129394531,	1,	'N',	-73.96067810058594,	40.77259826660156,  2,  16.5,	0, 	0.5,	0.3,	0,	0,	17.3),
        (1,'2016-03-20 19:20:23', '2016-03-20 19:23:25',	1,	0.6	,-73.94763946533203,	40.77100372314453,	1,	'N',	-73.95655059814453,	40.775543212890625, 1,	8.5,	0,	0.5,	0.3,	2.79,	0,	12.09),
        (1,'2016-03-20 19:20:25', '2016-03-20 19:31:51',	1,	2.9	,-73.98570251464844,	40.74380111694336,	1,	'N',	-73.96024322509766,	40.780582427978516, 2,	15.5,	0,	0.5,	0.3,	0,	0,	16.3)
            """.trimIndent()).toList().shouldHaveSize(1)
  }
}