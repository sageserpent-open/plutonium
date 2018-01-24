package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification

import scala.collection.immutable.Map
import scala.reflect.runtime.universe.{Super => _, This => _}

trait Timeline[EventId] {
  def revise(events: Map[EventId, Option[Event]]): Timeline[EventId]

  def retainUpTo(when: Unbounded[Instant]): Timeline[EventId]

  def itemCacheAt(when: Unbounded[Instant]): ItemCache
}

// TODO - given that we have 'emptyItemCache', I'm not sure if we need this too - let's see how it pans out...
object emptyTimeline {
  def apply[EventId]() = new TimelineImplementation[EventId]
}

object itemStateStorageUsingProxies extends ItemStateStorage {
  override protected type ItemSuperType = ItemExtensionApi
  override protected val clazzOfItemSuperType = classOf[ItemSuperType]

  override protected def uniqueItemSpecification(
      item: ItemSuperType): UniqueItemSpecification =
    item.uniqueItemSpecification
}
