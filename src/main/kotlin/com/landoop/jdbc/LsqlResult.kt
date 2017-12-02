package com.landoop.jdbc

class JsonJdbcRow(json: String) {
  private var jsonNode = JacksonJson.asJson(json)
}