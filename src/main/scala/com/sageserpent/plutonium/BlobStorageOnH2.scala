package com.sageserpent.plutonium

import cats.effect.IO
import com.sageserpent.plutonium.curium.DBResource
import scalikejdbc._

object BlobStorageOnH2 {
  def empty(connectionPool: ConnectionPool): BlobStorageOnH2 = ???

  def setupDatabaseTables(connectionPool: ConnectionPool): IO[Unit] =
    DBResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            sql"""
             CREATE TABLE ???(

             )
      """.update.apply()
          }
      })

  def dropDatabaseTables(connectionPool: ConnectionPool): IO[Unit] =
    DBResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            sql"""
             DROP ALL OBJECTS
           """.update.apply()
          }
      })
}

class BlobStorageOnH2(connectionPool: ConnectionPool)
    extends Timeline.BlobStorage {
  override implicit val timeOrdering: Ordering[ItemStateUpdateTime] = ???

  override def openRevision(): RevisionBuilder = ???

  override def timeSlice(when: ItemStateUpdateTime, inclusive: Boolean)
    : BlobStorage.Timeslice[ItemStateStorage.SnapshotBlob] = ???

  override def retainUpTo(when: ItemStateUpdateTime): Timeline.BlobStorage =
    ???
}
