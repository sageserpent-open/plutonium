package com.sageserpent.plutonium

import cats.effect.{IO, Resource}
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.Connection
import org.scalatest.{Outcome, TestSuite}

import scala.util.DynamicVariable

trait HBaseFixture extends TestSuite {
  private val hbaseTestingUtilityAccess
    : DynamicVariable[Option[HBaseTestingUtility]] = new DynamicVariable(None)

  protected def connection: Connection =
    hbaseTestingUtilityAccess.value.get.getConnection

  private def withHBaseRunning[Result](block: => Result): Result =
    Resource
      .make(IO {
        val hbaseTestingUtility = new HBaseTestingUtility()
        hbaseTestingUtility.startMiniCluster()
        hbaseTestingUtility
      })(hbaseTestingUtility =>
        IO {
          hbaseTestingUtility.shutdownMiniCluster()
      })
      .use(hbaseTestingUtility =>
        IO {
          hbaseTestingUtilityAccess.withValue(Some(hbaseTestingUtility)) {
            block
          }
      })
      .unsafeRunSync()

  override def withFixture(test: NoArgTest): Outcome = {
    withHBaseRunning(super.withFixture((test)))
  }
}
