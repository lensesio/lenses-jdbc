package com.landoop.jdbc

class LsqlResult(json: String) {
  private var jsonNode = JacksonJson.asJson(json)
}