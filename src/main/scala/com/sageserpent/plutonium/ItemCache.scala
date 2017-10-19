package com.sageserpent.plutonium

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Super => _, This => _, _}

trait ItemCache {
  def itemsFor[Item: TypeTag](id: Any): Stream[Item]

  def allItems[Item: TypeTag](): Stream[Item]

  def render[Item](bitemporal: Bitemporal[Item]): Stream[Item] = {
    bitemporal match {
      case ApBitemporalResult(preceedingContext,
                              stage: (Bitemporal[(_) => Item])) =>
        for {
          preceedingContext <- render(preceedingContext)
          stage             <- render(stage)
        } yield stage(preceedingContext)
      case PlusBitemporalResult(lhs, rhs) => render(lhs) ++ render(rhs)
      case PointBitemporalResult(item)    => Stream(item)
      case NoneBitemporalResult()         => Stream.empty
      case bitemporal @ IdentifiedItemsBitemporalResult(id) =>
        implicit val typeTag = bitemporal.capturedTypeTag
        itemsFor(id)
      case bitemporal @ WildcardBitemporalResult() =>
        implicit val typeTag = bitemporal.capturedTypeTag
        allItems()
    }
  }

  def numberOf[Item](bitemporal: Bitemporal[Item]): Int = {
    bitemporal match {
      case ApBitemporalResult(preceedingContext,
                              stage: (Bitemporal[(_) => Item])) =>
        numberOf(preceedingContext) * numberOf(stage)
      case PlusBitemporalResult(lhs, rhs) => numberOf(lhs) + numberOf(rhs)
      case PointBitemporalResult(item)    => 1
      case NoneBitemporalResult()         => 0
      case bitemporal @ IdentifiedItemsBitemporalResult(id) =>
        implicit val typeTag = bitemporal.capturedTypeTag
        itemsFor(id).size
      case bitemporal @ WildcardBitemporalResult() =>
        implicit val typeTag = bitemporal.capturedTypeTag
        allItems().size
    }
  }
}

object emptyItemCache extends ItemCache {
  override def itemsFor[Item: TypeTag](id: Any): Stream[Item] = Stream.empty

  override def allItems[Item: TypeTag](): Stream[Item] =
    Stream.empty
}
