package com.landoop.jdbc4.client.domain

data class Table(val name: String,
                 val keyType: String,
                 val valueType: String,
                 val keySchema: String?,
                 val valueSchema: String?)