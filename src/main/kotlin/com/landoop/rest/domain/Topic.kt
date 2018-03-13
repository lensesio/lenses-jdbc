package com.landoop.rest.domain

data class Topic(
    val name: String,
    val keyType: String,
    val keySchema: String?,
    val valueType: String,
    val valueSchema: String?
)