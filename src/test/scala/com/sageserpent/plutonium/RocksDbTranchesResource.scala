package com.sageserpent.plutonium

import cats.effect.{IO, Resource}
import com.sageserpent.curium.ImmutableObjectStorage.Tranches
import com.sageserpent.curium.RocksDbTranches
import com.sageserpent.plutonium.RocksDbTranchesResource.TrancheId

trait TranchesResource[TrancheId] {
  val tranchesResource: Resource[IO, Tranches[TrancheId]]
}

object RocksDbTranchesResource {
  type TrancheId = RocksDbTranches#TrancheId
}

trait RocksDbTranchesResource
    extends TranchesResource[RocksDbTranchesResource.TrancheId]
    with RocksDbResource {
  override val tranchesResource: Resource[IO, Tranches[TrancheId]] = for {
    rocksDb <- rocksDbResource
  } yield new RocksDbTranches(rocksDb)
}
