package io.lenses.jdbc4

import arrow.core.None
import arrow.core.Some
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import io.lenses.jdbc4.client.JdbcRequestMessage

class JdbcRequestMessageTest : WordSpec() {
  init {
    "JdbcRequestMessage" should {
      "convert to and from json" {
        val msg = JdbcRequestMessage("SELECT * FROM abc", "token1")
        val json = JacksonSupport.toJson(msg)
        val actual = JacksonSupport.fromJson<JdbcRequestMessage>(json)
        actual shouldBe msg
      }
    }
  }
}