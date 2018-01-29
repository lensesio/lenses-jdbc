package com.landoop.rest.domain

data class Table(val name: String,
                 val keyType: String,
                 val valueType: String,
                 val keySchema: String?,
                 val valueSchema: String?)