package com.sageserpent.plutonium

import java.util.UUID

import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification

object itemStateStorageUsingProxies extends ItemStateStorage {
  override protected type ItemSuperType = ItemExtensionApi
  override protected val clazzOfItemSuperType = classOf[ItemSuperType]

  override protected def uniqueItemSpecification(
      item: ItemSuperType): UniqueItemSpecification =
    item.uniqueItemSpecification

  override protected def lifecycleUUID(item: ItemSuperType): UUID =
    item.asInstanceOf[LifecycleUUIDApi].lifecycleUUID

  override protected def itemStateUpdateKey(
      item: ItemExtensionApi): Option[ItemStateUpdateKey] =
    item.asInstanceOf[ItemStateUpdateKeyTrackingApi].itemStateUpdateKey

  override protected def noteAnnihilationOnItem(item: ItemSuperType): Unit = {
    item
      .asInstanceOf[AnnihilationHook]
      .recordAnnihilation()
  }
}
