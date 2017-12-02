package com.landoop.jdbc.domain

data class LsqlData(val topic:String?, val data: ArrayList<String>, private val schema: String?)
