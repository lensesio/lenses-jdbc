package com.landoop.jdbc

import com.landoop.jdbc4.JacksonSupport

class JsonJdbcRow(json: String) {
  private var jsonNode = JacksonSupport.asJson(json)
}