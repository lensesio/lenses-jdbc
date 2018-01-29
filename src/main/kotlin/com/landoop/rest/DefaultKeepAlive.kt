package com.landoop.rest

import org.apache.http.HttpResponse
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy
import org.apache.http.protocol.HttpContext

object DefaultKeepAlive : DefaultConnectionKeepAliveStrategy() {
  override fun getKeepAliveDuration(response: HttpResponse?, context: HttpContext?): Long {
    var keepAlive = super.getKeepAliveDuration(response, context)
    if (keepAlive == -1L) {
      // Keep connections alive 5 seconds if a keep-alive value
      // has not be explicitly set by the server
      keepAlive = 5000
    }
    return keepAlive
  }
}