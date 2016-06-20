package com.sageserpent.plutonium

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Created by Gerard on 18/06/2016.
  */
trait AkkaSetup extends BeforeAndAfterAll {
  this: Suite =>
  import AkkaSetup._

  var akkaStdOutLogLevel = "OFF"
  var akkaLogLevel = "ERROR"

  var akkaSystem: ActorSystem = null

  override protected def beforeAll() = {
    super.beforeAll()

    val config = ConfigFactory.load()
    akkaStdOutLogLevel = config.getString(akkaStdoutLogLevelKey)
    akkaLogLevel = config.getString(akkaLogLevelKey)

    System.setProperty(akkaStdoutLogLevelKey, "OFF")
    System.setProperty(akkaLogLevelKey, "ERROR")

    ConfigFactory.invalidateCaches()

    akkaSystem = akka.actor.ActorSystem()
  }

  override protected  def afterAll() = {
    akkaSystem.terminate()

    System.setProperty(akkaStdoutLogLevelKey, akkaStdOutLogLevel)
    System.setProperty(akkaLogLevelKey, akkaLogLevel)

    ConfigFactory.invalidateCaches()

    super.afterAll()
  }
}

object AkkaSetup {
  val akkaStdoutLogLevelKey: String = "akka.stdout-loglevel"
  val akkaLogLevelKey: String = "akka.loglevel"
}