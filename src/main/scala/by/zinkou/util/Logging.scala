package by.zinkou.util

import biz.paluch.logging.gelf.log4j.GelfLogAppender


object Logging {
  def createGelfLogAppender(host: String, port: Int): GelfLogAppender = {
    val appender = new GelfLogAppender()
    appender.setHost(host)
    appender.setPort(port)
    appender.setExtractStackTrace("true")
    appender.setFilterStackTrace(false)
    appender.setMaximumMessageSize(8192)
    appender.setIncludeFullMdc(true)
    appender.activateOptions()

    appender
  }
}
