package com.sageserpent.plutonium

import com.sageserpent.plutonium.BlobStorage.LifecycleIndex

trait AnnihilationHook {
  def recordAnnihilation(): Unit

  def setLifecycleIndex(lifecycleIndex: LifecycleIndex): Unit
}
