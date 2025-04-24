package com.sageserpent.plutonium

import cats.Applicative

import scala.reflect.runtime.universe._

/** Selects items from an [[ItemCache]], which for client code is typically a
  * [[Scope]].<p>A [[Bitemporal]] supports applicative operations, so
  * computations requiring combinations or alternations of items are possible;
  * typically these are built up from queries on an [[ItemCache]] or [[Scope]].
  * @tparam Item
  */
sealed trait Bitemporal[Item] {
  def map[Item2]: (Item => Item2) => Bitemporal[Item2] =
    implicitly[Applicative[Bitemporal]].map[Item, Item2](this)

  def ap[Item2](stage: Bitemporal[Item => Item2]): Bitemporal[Item2] =
    ApBitemporalResult(precedingContext = this, stage = stage)

  def plus(another: Bitemporal[Item]): Bitemporal[Item] =
    PlusBitemporalResult(lhs = this, rhs = another)
}

private case class ApBitemporalResult[ContextItem, Item](
    precedingContext: Bitemporal[ContextItem],
    stage: Bitemporal[ContextItem => Item]
) extends Bitemporal[Item]

private case class PlusBitemporalResult[Item](
    lhs: Bitemporal[Item],
    rhs: Bitemporal[Item]
) extends Bitemporal[Item]

private case class PointBitemporalResult[Item](item: Item)
    extends Bitemporal[Item]

private case class NoneBitemporalResult[Item]() extends Bitemporal[Item]

private case class IdentifiedItemsBitemporalResult[Item](
    uniqueItemSpecification: UniqueItemSpecification
) extends Bitemporal[Item]

private case class WildcardBitemporalResult[Item](clazz: Class[Item])
    extends Bitemporal[Item]

object Bitemporal {

  /** Lift a value into a [[Bitemporal]]. This allows values that are not items
    * to be injected into a client-side computation of type [[Bitemporal]].
    * @param value
    *   Value to inject.
    * @return
    *   A [[Bitemporal]] that injects the value into calls to [[Bitemporal.map]]
    *   etc.
    */
  def apply[Value](value: Value): Bitemporal[Value] = PointBitemporalResult(
    value
  )

  /** @param id
    * @param clazz
    * @tparam Item
    * @return
    *   A [[Bitemporal]] that selects items by id and assignment compatible
    *   class.
    */
  def withId[Item](id: Any, clazz: Class[Item]): Bitemporal[Item] =
    IdentifiedItemsBitemporalResult(UniqueItemSpecification(id, clazz))

  /** @param id
    * @tparam Item
    * @return
    *   A [[Bitemporal]] that selects items by id and assignment compatible
    *   class.
    */
  def withId[Item: TypeTag](id: Any): Bitemporal[Item] =
    IdentifiedItemsBitemporalResult(UniqueItemSpecification(id, typeOf[Item]))

  /** @param clazz
    * @tparam Item
    * @return
    *   A [[Bitemporal]] that selects items by assignment compatible class.
    */
  def wildcard[Item](clazz: Class[Item]): Bitemporal[Item] =
    WildcardBitemporalResult[Item](clazz)

  /** @tparam Item
    * @return
    *   A [[Bitemporal]] that selects items by assignment compatible class.
    */
  def wildcard[Item: TypeTag](): Bitemporal[Item] =
    WildcardBitemporalResult[Item](classFromType(typeOf[Item]))

  /** @tparam Item
    * @return
    *   A [[Bitemporal]] that selects no items at all.
    */
  def none[Item]: Bitemporal[Item] = NoneBitemporalResult[Item]()

  // TODO - a Bitemporal[Instant] that yields the query scope's 'when' from
  // within the monad.

  // TODO - something that yields a bitemporal for the asOf and for the
  // revision?

  // TODO - something that takes a bitemporal and then makes a time-shifted
  // bitemporal from within the monad - for PnL.
}
