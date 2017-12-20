package com.sageserpent.plutonium
import com.sageserpent.plutonium.BlobStorage.UniqueItemSpecification

protected[plutonium] trait Recorder {
  def itemReconstitutionData: UniqueItemSpecification
}
