package com.sageserpent.plutonium

import scala.reflect.runtime.universe.TypeTag

object ItemStateStorage {
  import BlobStorage._

  def snapshotFor[Item: TypeTag](item: Item): SnapshotBlob = ???

  trait ReconstitutionContext extends ItemCache {
    val blobStorageTimeslice: BlobStorage.Timeslice // NOTE: abstracting this allows the prospect of a 'moving' timeslice for use when executing an update plan.

    override def itemsFor[Item: TypeTag](id: Any): Stream[Item] =
      for {
        uniqueItemSpecification <- blobStorageTimeslice.uniqueItemQueriesFor(id)
      } yield itemFor(uniqueItemSpecification)

    override def allItems[Item: TypeTag](): Stream[Item] =
      for {
        uniqueItemSpecification <- blobStorageTimeslice
          .uniqueItemQueriesFor[Item]
      } yield itemFor(uniqueItemSpecification)

    // This has a precondition that the type tag must pick out precisely one item - zero or multiple is not permitted.
    protected def itemFor[Item: TypeTag](
        uniqueItemSpecification: UniqueItemSpecification): Item = ???
  }
}
