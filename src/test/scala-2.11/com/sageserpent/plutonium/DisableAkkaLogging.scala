package com.sageserpent.plutonium

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Created by Gerard on 18/06/2016.
  */
trait DisableAkkaLogging extends BeforeAndAfterAll {
  this: Suite =>
  import DisableAkkaLogging._

  var akkaStdOutLogging = "OFF"
  var akkaLogLevel = "ERROR"
  var akkaDeadLettersLogging = 0
  var akkaDeadLettersDuringShutdownLogging = false

  override protected def beforeAll() = {
    super.beforeAll()

    val config = ConfigFactory.load()
    akkaStdOutLogging = config.getString(akkaStdoutLogLevelKey)
    akkaLogLevel = config.getString(akkaLogLevelKey)
    akkaDeadLettersLogging = config.getInt(akkaDeadLettersKey)
    akkaDeadLettersDuringShutdownLogging = config.getBoolean(akkaDeadLettersDuringShutdownKey)

    System.setProperty(akkaStdoutLogLevelKey, "OFF")
    System.setProperty(akkaLogLevelKey, "ERROR")
    System.setProperty(akkaDeadLettersKey, 0.toString)
    System.setProperty(akkaDeadLettersDuringShutdownKey, false.toString)

    ConfigFactory.invalidateCaches()
  }

  override protected  def afterAll() = {
    System.setProperty(akkaStdoutLogLevelKey, akkaStdOutLogging)
    System.setProperty(akkaLogLevelKey, akkaLogLevel)
    System.setProperty(akkaDeadLettersKey, akkaDeadLettersLogging.toString)
    System.setProperty(akkaDeadLettersDuringShutdownKey, akkaDeadLettersDuringShutdownLogging.toString)

    ConfigFactory.invalidateCaches()

    super.afterAll()
  }
}

object DisableAkkaLogging {
  val akkaStdoutLogLevelKey: String = "akka.stdout-loglevel"
  val akkaLogLevelKey: String = "akka.loglevel"
  val akkaDeadLettersKey: String = "akka.log-dead-letters"
  val akkaDeadLettersDuringShutdownKey: String = "akka.log-dead-letters-during-shutdown"
}