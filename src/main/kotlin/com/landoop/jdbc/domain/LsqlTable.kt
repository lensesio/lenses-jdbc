package com.landoop.jdbc.domain

data class LsqlTable(val name: String,
                     val keyType: String,
                     val valueType: String,
                     val keySchema: String?,
                     val valueSchema: String?)