package com.landoop.rest.domain

data class Topic(
    val topicName: String,
    val keyType: String,
    val valueType: String,
    val messagesPerSecond: Int,
    val timestamp: Long,
    val totalMessages: Long,
    val replication: Int,
    val isMarkedForDeletion: Boolean,
    val isControlTopic: Boolean
)