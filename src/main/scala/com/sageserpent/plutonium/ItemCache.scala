package com.sageserpent.plutonium

import cats.Alternative
import cats.implicits._

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

/** Provides access to items selected by instances of [[Bitemporal]].
  * @todo
  *   Remove this, or make it package-private. Client code just wants to work
  *   with [[Scope]].
  */
trait ItemCache {

  /** @param bitemporal
    *   Specifies what item or items to retrieve.
    * @tparam Item
    * @return
    *   A lazy list of matching items, there may be none.
    */
  def render[Item](bitemporal: Bitemporal[Item]): LazyList[Item]

  /** Alternative to [[render]] that yields a Java [[java.lang.Iterable]].
    * @param bitemporal
    *   Specifies what item or items to retrieve.
    * @tparam Item
    * @return
    *   An iterable over matching items; there may be none.
    */
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
  def render[Item](bitemporal: Bitemporal[Item]): LazyList[Item] = {
    bitemporal match {
      case FlatMapBitemporalResult(
            precedingContext,
            stage
          ) =>
        render(precedingContext)
          .flatMap(precedingContext => render(stage(precedingContext)))
      case TailRecMResult(initialItem, stage) =>
        @tailrec
        def unroll(
            intermediates: LazyList[Any],
            results: LazyList[Item]
        ): LazyList[Item] = if (intermediates.nonEmpty) {
          val (newIntermediates, newResults) = Alternative[LazyList].separate(
            intermediates.flatMap(intermediate => render(stage(intermediate)))
          )

          unroll(newIntermediates, results.force lazyAppendedAll newResults)
        } else results

        unroll(LazyList(initialItem), LazyList.empty)
      case PlusBitemporalResult(lhs, rhs) => render(lhs) ++ render(rhs)
      case PointBitemporalResult(item)    => LazyList(item)
      case NoneBitemporalResult()         => LazyList.empty
      case IdentifiedItemsBitemporalResult(uniqueItemSpecification) =>
        itemsFor(uniqueItemSpecification)
      case WildcardBitemporalResult(clazz) =>
        allItems(clazz)
    }
  }

  def numberOf[Item](bitemporal: Bitemporal[Item]): Int = {
    bitemporal match {
      case FlatMapBitemporalResult(
            precedingContext,
            stage
          ) =>
        render(precedingContext).map(input => numberOf(stage(input))).sum
      case TailRecMResult(initialItem, stage) =>
        @tailrec
        def unroll(
            intermediates: LazyList[Any],
            count: Int
        ): Int = if (intermediates.nonEmpty) {
          val (newIntermediates, newResults) = Alternative[LazyList].separate(
            intermediates.flatMap(intermediate => render(stage(intermediate)))
          )

          unroll(newIntermediates, count + newResults.size)
        } else count

        unroll(LazyList(initialItem), 0)
      case PlusBitemporalResult(lhs, rhs) => numberOf(lhs) + numberOf(rhs)
      case PointBitemporalResult(_)       => 1
      case NoneBitemporalResult()         => 0
      case IdentifiedItemsBitemporalResult(uniqueItemSpecification) =>
        itemsFor(uniqueItemSpecification).size
      case WildcardBitemporalResult(clazz) =>
        allItems(clazz).size
    }
  }

  protected def itemsFor[Item](
      uniqueItemSpecification: UniqueItemSpecification
  ): LazyList[Item]

  protected def allItems[Item](clazz: Class[Item]): LazyList[Item]
}
