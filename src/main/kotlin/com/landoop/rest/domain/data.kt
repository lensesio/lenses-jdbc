package com.landoop.rest.domain

data class Message(
    val timestamp: Long,
    val partition: Int,
    val key: String,
    val offset: Long,
    val topic: String,
    val value: String)

data class JdbcData(
    val topic: String?,
    val data: List<String>,
    val schema: String?
)

data class InsertResponse(val name: String)

data class PreparedInsertBody(val topic: String, val records: List<InsertRecord>)

data class InsertRecord(val key: String, val value: String)

data class PreparedInsertResponse(val info: PreparedInsertInfo?,
                                  val error: String?)

data class PreparedInsertInfo(val topic: String,
                              val fields: List<InsertField>,
                              val keyType: String,
                              val valueType: String,
                              val keySchema: String?,
                              val valueSchema: String)

data class InsertField(val name: String, val parents: List<String>, val isKey: Boolean)