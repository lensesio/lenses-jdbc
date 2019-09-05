package io.lenses.jdbc4

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.io.InputStream

object JacksonSupport {

  val mapper: ObjectMapper = ObjectMapper().apply {
    this.registerModule(KotlinModule())
    this.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    this.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    this.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
    this.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
    this.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
    this.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
  }

  fun <T> toJson(t: T): String = io.lenses.jdbc4.JacksonSupport.mapper.writeValueAsString(t)

  inline fun <reified T : Any> fromJson(json: String): T {
    return io.lenses.jdbc4.JacksonSupport.mapper.readValue<T>(json, T::class.java)
  }

  inline fun <reified T : Any> fromJson(stream: InputStream): T {
    return io.lenses.jdbc4.JacksonSupport.mapper.readValue<T>(stream, T::class.java)
  }
}