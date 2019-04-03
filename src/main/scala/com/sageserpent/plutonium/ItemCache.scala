package com.sageserpent.plutonium

trait ItemCache {
  // Why a stream for the result type? - two reasons that overlap - we may have no instance in force for the scope, or we might have several that share the same id, albeit with
  // different runtime subtypes of 'Item'. What's more, if 'bitemporal' was cooked using 'Bitemporal.wildcard', we'll have every single instance of a runtime subtype of 'Item'.
  def render[Item](bitemporal: Bitemporal[Item]): Stream[Item]

  def numberOf[Item](bitemporal: Bitemporal[Item]): Int
}

trait ItemCacheImplementation extends ItemCache {
  protected def itemsFor[Item](
      uniqueItemSpecification: UniqueItemSpecification): Stream[Item]

  protected def allItems[Item](clazz: Class[Item]): Stream[Item]

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
      case IdentifiedItemsBitemporalResult(uniqueItemSpecification) =>
        itemsFor(uniqueItemSpecification)
      case WildcardBitemporalResult(clazz) =>
        allItems(clazz)
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
      case IdentifiedItemsBitemporalResult(uniqueItemSpecification) =>
        itemsFor(uniqueItemSpecification).size
      case WildcardBitemporalResult(clazz) =>
        allItems(clazz).size
    }
  }
}

object emptyItemCache extends ItemCacheImplementation {
  override def itemsFor[Item](
      uniqueItemSpecification: UniqueItemSpecification): Stream[Item] =
    Stream.empty

  override def allItems[Item](clazz: Class[Item]): Stream[Item] =
    Stream.empty
}
