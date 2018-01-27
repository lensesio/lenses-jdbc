package com.landoop.rest.domain

data class Topic(
    val keyType: String,
    val messagesPerSecond: Int,
    val timestamp: Long,
    val valueType: String,
    val config: List<ConfigEntry>?,
    val totalMessages: Long,
    val replication: Int,
    val topicName: String,
    val isMarkedForDeletion: Boolean,
    val partitions: Int,
    val isControlTopic: Boolean,
    val messagesPerPartition: List<MessagesPerPartition>?
)

data class ConfigEntry(val configuration: String, val value: String, val defaultValue: String, val documentation: String)
data class MessagesPerPartition(val partition: Int,
                                val messages: Long,
                                val begin: Long,
                                val end: Long)
