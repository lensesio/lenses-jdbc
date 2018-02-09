package com.landoop.rest.domain

import java.util.*

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

data class PreparedInsertStatementResponse(val info: PreparedInsertStatementInfo?,
                                           val error: String?)

data class PreparedInsertStatementInfo(val topic: String,
                                       val fields: List<SqlInsertField>,
                                       val keyType: String,
                                       val valueType: String,
                                       val keySchema: String?,
                                       val valueSchema: String?)

data class SqlInsertField(val name: String, val parents: List<String>, val isKey: Boolean)