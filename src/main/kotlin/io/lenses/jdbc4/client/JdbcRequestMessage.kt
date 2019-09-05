package io.lenses.jdbc4.client

data class JdbcRequestMessage(val sql: String,
                              val token: String)