package com.sageserpent.plutonium

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Super => _, This => _, _}

/**
  * Created by gerardMurphy on 14/05/2017.
  */
trait ItemCache {
  def itemsFor[Item <: Identified: TypeTag](id: Item#Id): Stream[Item]

  def allItems[Item <: Identified: TypeTag](): Stream[Item]

  def render[Item](bitemporal: Bitemporal[Item]): Stream[Item] = {
    def zeroOrOneItemFor[Item <: Identified: TypeTag](
        id: Item#Id): Stream[Item] = {
      itemsFor(id) match {
        case zeroOrOneItems @ (Stream.Empty | _ #:: Stream.Empty) =>
          zeroOrOneItems
        case _ =>
          throw new scala.RuntimeException(
            s"Id: '${id}' matches more than one item of type: '${typeTag.tpe}'.")
      }
    }

    def singleItemFor[Item <: Identified: TypeTag](id: Item#Id): Stream[Item] = {
      zeroOrOneItemFor(id) match {
        case Stream.Empty =>
          throw new scala.RuntimeException(
            s"Id: '${id}' does not match any items of type: '${typeTag.tpe}'.")
        case result @ Stream(_) => result
      }
    }

    bitemporal match {
      case ApBitemporalResult(preceedingContext,
                              stage: (Bitemporal[(_) => Item])) =>
        for {
          preceedingContext <- render(preceedingContext)
          stage             <- render(stage)
        } yield stage(preceedingContext)
      case PlusBitemporalResult(lhs, rhs) => render(lhs) ++ render(rhs)
      case PointBitemporalResult(raw)     => Stream(raw)
      case NoneBitemporalResult()         => Stream.empty
      case bitemporal @ IdentifiedItemsBitemporalResult(id) =>
        implicit val typeTag = bitemporal.capturedTypeTag
        itemsFor(id)
      case bitemporal @ ZeroOrOneIdentifiedItemBitemporalResult(id) =>
        implicit val typeTag = bitemporal.capturedTypeTag
        zeroOrOneItemFor(id)
      case bitemporal @ SingleIdentifiedItemBitemporalResult(id) =>
        implicit val typeTag = bitemporal.capturedTypeTag
        singleItemFor(id)
      case bitemporal @ WildcardBitemporalResult() =>
        implicit val typeTag = bitemporal.capturedTypeTag
        allItems()
    }
  }

  def numberOf[Item <: Identified: TypeTag](id: Item#Id): Int =
    itemsFor(id).size
}

object emptyItemCache extends ItemCache {
  override def itemsFor[Item <: Identified: universe.TypeTag](
      id: Item#Id): Stream[Item] = Stream.empty

  override def allItems[Item <: Identified: universe.TypeTag](): Stream[Item] =
    Stream.empty
}
