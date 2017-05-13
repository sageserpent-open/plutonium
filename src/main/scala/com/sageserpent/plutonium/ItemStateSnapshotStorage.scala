package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 01/05/2017.
  */
trait ItemStateSnapshotStorage[+EventId] {
  def snapshotsFor[Item <: Identified: TypeTag](
      id: Item#Id,
      when: Unbounded[Instant],
      exclusions: Set[TypeTag[_ <: Item]]): Stream[ItemStateSnapshot]

  def openRevision[NewEventId >: EventId]()
    : ItemStateSnapshotRevisionBuilder[NewEventId]

  def idsFor[Item <: Identified: TypeTag]: Stream[Item#Id]
}

object noItemStateSnapshots extends ItemStateSnapshotStorage[Nothing] {
  override def snapshotsFor[Item <: Identified: TypeTag](
      id: Item#Id,
      when: Unbounded[Instant],
      exclusions: Set[TypeTag[_ <: Item]]): Stream[ItemStateSnapshot] =
    Stream.empty

  override def openRevision[NewEventId]()
    : ItemStateSnapshotRevisionBuilder[NewEventId] = ???

  override def idsFor[Item <: Identified: TypeTag]: Stream[Item#Id] =
    Stream.empty
}
