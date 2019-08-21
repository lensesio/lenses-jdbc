package io.lenses.jdbc4.client.domain

import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class StreamingSelectResult {

  private val endOfRecords = "___end"

  // holds records as they are received
  private val records = LinkedBlockingQueue<String>()

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

  fun endStream() {
    // If the schema has not been set (no records for example)
    // then we must complete it now with null.
    if (schema.get() == null) {
      setSchema(null)
    }
    // we also need to release anyone waiting on the blocking queue know we are done
    this.records.put(endOfRecords)
    // we might still be waiting on data, so now we can answer that question
    isReady.countDown()
    isReady.countDown()
  }

  fun addRecord(record: String) {
    records.put(record)
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
  private fun isReady(timeout: Long, unit: TimeUnit) {
    isReady.await(timeout, unit)
  }

  /**
   * A Blocking call that returns the next record from the buffer.
   * If the buffer is empty, it will block, until either a record is
   * received, or we hit end of stream.
   * At the end of stream we will return null.
   */
  fun next(): String? {
    val record = records.take()
    if (error.get() != null)
      throw error.get()
    return if (record == endOfRecords) {
      // we must put this back on in case of multiple readers
      records.put(endOfRecords)
      null
    } else record
  }
}