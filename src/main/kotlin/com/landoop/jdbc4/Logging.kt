package com.landoop.jdbc4

import org.slf4j.Logger
import org.slf4j.LoggerFactory

interface Logging {

  val logger: Logger
    get() = LoggerFactory.getLogger(javaClass)

}