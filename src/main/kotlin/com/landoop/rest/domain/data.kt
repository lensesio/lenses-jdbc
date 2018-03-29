package com.landoop.rest.domain

import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.Schema
import java.sql.SQLException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

data class Message(
    val timestamp: Long,
    val partition: Int,
    val key: String,
    val offset: Long,
    val topic: String,
    val value: String)

data class SelectResult(
    val data: List<String>,
    val schema: String?
)

class StreamingSelectResult {

  val endOfRecords = "___end"

  // holds records as they are received
  val records = LinkedBlockingQueue<String>()

  // whether we ever received data, obviously the queue may have been emptied by reading from it
  // and so not always accurate if we did have data at all
  private val receivedREcords = AtomicBoolean(false)

  private val completed = AtomicBoolean(false)

  private val error = AtomicReference<Throwable>(null)

  // holds the single String schema
  val schema = AtomicReference<String?>(null)

  // triggered once we can definitely say if we have a schema or not
  private val hasSchema = CountDownLatch(1)

  private val isReady = CountDownLatch(2)

  fun setSchema(schema: String?) {
    this.schema.set(schema)
    hasSchema.countDown()
    isReady.countDown()
  }

  /**
   * Blocking call, which will wait until the schema has arrived
   * or the result is completed
   */
  fun getSchema(): String? {
    hasSchema.await(1, TimeUnit.DAYS)
    return schema.get()
  }

  // returns true if this resultset currently has data
  // blocks until ready
  fun hasData(timeout: Long, unit: TimeUnit): Boolean {
    isReady(timeout, unit)
    return records.peek() != endOfRecords
  }

  fun complete() {
    // If the schema has not been set (no records for example)
    // then we must complete it now with null.
    if (schema.get() == null) {
      setSchema(null)
    }
    // we also need to let readers of records know we are done
    this.records.put(endOfRecords)
    this.completed.set(true)
    // we might still be waiting on data, so now we can answer that question
    isReady.countDown()
    isReady.countDown()
  }

  fun addRecord(record: String) {
    records.put(record)
    receivedREcords.set(true)
    isReady.countDown()
  }

  fun setError(t: Throwable) {
    error.set(t)
  }

  fun error(): Throwable? = error.get()

  /**
   * A blocking call that will wait until the streaming result can answer
   * the question - is there any data?
   *
   * It can do this in two ways:
   *
   * 1. A record and schema have been received. Clearly then the answer is yes, it doesn't
   * need to wait for the rest of the data.
   *
   * 2. The end of the stream has been reached. Then the answer is no.
   */
  fun isReady(timeout: Long, unit: TimeUnit) {
    isReady.await(timeout, unit)
  }

  /**
   * A non-blocking call that answers the question - is the underlying
   * stream completed.
   */
  fun isComplete(): Boolean = completed.get()
}

data class InsertResponse(val name: String)

data class PreparedInsertBody(val topic: String, val records: List<InsertRecord>)

data class InsertRecord(val key: JsonNode, val value: JsonNode)

data class PreparedInsertResponse(val info: PreparedInsertInfo?,
                                  val error: String?)

data class PreparedInsertInfo(val topic: String,
                              val fields: List<InsertField>,
                              val keyType: String,
                              val valueType: String,
                              val keySchema: String?,
                              val valueSchema: String) {

  // returns an avro field that matches the field identified by the parameter index
  // if the field we are asking for is the key then it will return a string type
  // todo support more than string key types
  // 0-indexed
  fun schemaField(param: Int): Schema.Field {
    val parsedSchema = Schema.Parser().parse(valueSchema)
    val field = fields[param]
    return if (field.isKey) {
      Schema.Field(field.name, Schema.create(Schema.Type.STRING), null, null)
    } else {
      // get the schema for our immediate parent, if we have parents, by traversing the tree
      val schema = field.parents.fold(parsedSchema, { schema, part -> schema.getField(part).schema() })
      schema.getField(field.name) ?: throw SQLException("Could not find field $param in ${parsedSchema.fields}")
    }
  }

  /**
   * @return true if the field given by the index is a key field
   * 0-indexed
   */
  fun isKey(param: Int): Boolean = fields[param].isKey
}

data class InsertField(val name: String, val parents: List<String>, val isKey: Boolean) {

  data class Path(private val parts: List<String>) {
    override fun toString(): String = parts.joinToString(".")
  }

  fun path(): Path {
    val parents = this.parents.toMutableList()
    parents.add(this.name)
    return Path(parents)
  }
}