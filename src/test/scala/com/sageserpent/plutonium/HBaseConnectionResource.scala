package com.sageserpent.plutonium

import cats.effect.{IO, Resource}
import org.apache.hadoop.hbase.client.Connection

trait HBaseConnectionResource extends HBaseFixture {
  val connectionResource: Resource[IO, Connection] = Resource.make(IO {
    connection
  })(connection => IO { connection.close() })
}
