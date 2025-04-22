package com.sageserpent.plutonium

import scala.collection.JavaConverters._

/** Provides access to items selected by instances of [[Bitemporal]].
  */
trait ItemCache {

  /** @param bitemporal
    *   Specifies what item or items to retrieve.
    * @tparam Item
    * @return
    *   A stream of matching items, which may be empty.
    */
  def render[Item](bitemporal: Bitemporal[Item]): Stream[Item]

  def renderAsIterable[Item](
      bitemporal: Bitemporal[Item]
  ): java.lang.Iterable[Item] =
    render(bitemporal).asJava

  /** @param bitemporal
    *   Specifies what items to count.
    * @tparam Item
    * @return
    *   The number of the matching items, which may be zero.
    */
  def numberOf[Item](bitemporal: Bitemporal[Item]): Int

  def numberOf[Item](id: Any, clazz: Class[Item]): Int =
    numberOf(Bitemporal.withId(id, clazz))
}

protected[plutonium] trait ItemCacheImplementation extends ItemCache {
  def render[Item](bitemporal: Bitemporal[Item]): Stream[Item] = {
    bitemporal match {
      case ApBitemporalResult(
            preceedingContext,
            stage: (Bitemporal[(_) => Item])
          ) =>
        for {
          preceedingContext <- render(preceedingContext)
          stage             <- render(stage)
        } yield stage(preceedingContext)
      case PlusBitemporalResult(lhs, rhs) => render(lhs) ++ render(rhs)
      case PointBitemporalResult(item)    => Stream(item)
      case NoneBitemporalResult()         => Stream.empty
      case IdentifiedItemsBitemporalResult(uniqueItemSpecification) =>
        itemsFor(uniqueItemSpecification)
      case WildcardBitemporalResult(clazz) =>
        allItems(clazz)
    }
  }

  def numberOf[Item](bitemporal: Bitemporal[Item]): Int = {
    bitemporal match {
      case ApBitemporalResult(
            preceedingContext,
            stage: (Bitemporal[(_) => Item])
          ) =>
        numberOf(preceedingContext) * numberOf(stage)
      case PlusBitemporalResult(lhs, rhs) => numberOf(lhs) + numberOf(rhs)
      case PointBitemporalResult(item)    => 1
      case NoneBitemporalResult()         => 0
      case IdentifiedItemsBitemporalResult(uniqueItemSpecification) =>
        itemsFor(uniqueItemSpecification).size
      case WildcardBitemporalResult(clazz) =>
        allItems(clazz).size
    }
  }

  protected def itemsFor[Item](
      uniqueItemSpecification: UniqueItemSpecification
  ): Stream[Item]

  protected def allItems[Item](clazz: Class[Item]): Stream[Item]
}
