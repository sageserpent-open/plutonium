package com.sageserpent.plutonium

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Created by Gerard on 18/06/2016.
  */
trait DisableAkkaLogging extends BeforeAndAfterAll {
  this: Suite =>
  import DisableAkkaLogging._

  var akkaStdOutLogLevel = "OFF"
  var akkaLogLevel = "ERROR"

  override protected def beforeAll() = {
    super.beforeAll()

    val config = ConfigFactory.load()
    akkaStdOutLogLevel = config.getString(akkaStdoutLogLevelKey)
    akkaLogLevel = config.getString(akkaLogLevelKey)

    System.setProperty(akkaStdoutLogLevelKey, "OFF")
    System.setProperty(akkaLogLevelKey, "ERROR")

    ConfigFactory.invalidateCaches()
  }

  override protected  def afterAll() = {
    System.setProperty(akkaStdoutLogLevelKey, akkaStdOutLogLevel)
    System.setProperty(akkaLogLevelKey, akkaLogLevel)

    ConfigFactory.invalidateCaches()

    super.afterAll()
  }
}

object DisableAkkaLogging {
  val akkaStdoutLogLevelKey: String = "akka.stdout-loglevel"
  val akkaLogLevelKey: String = "akka.loglevel"
}