package com.landoop.rest.domain

data class Messages(
    val timestamp: Long,
    val partition: Int,
    val key: String,
    val offset: Long,
    val topic: String,
    val value: String)