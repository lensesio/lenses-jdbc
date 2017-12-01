package com.landoop.jdbc.domain

data class JdbcData(val topic:String, val data: ArrayList<String>, private val schema: String?)
