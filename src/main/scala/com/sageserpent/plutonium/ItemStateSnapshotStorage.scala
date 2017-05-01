package com.sageserpent.plutonium

import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 01/05/2017.
  */
trait ItemStateSnapshotStorage extends ItemIdQueryApi {
  def snapshotsFor[Item <: Identified: TypeTag](
      id: Item#Id,
      exclusions: Set[TypeTag[_ <: Item]]): Stream[ItemStateSnapshot]

  def openRevision(): ItemStateSnapshotRevisionBuilder

  def fork(scope: javaApi.Scope): ItemStateSnapshotStorage
}
