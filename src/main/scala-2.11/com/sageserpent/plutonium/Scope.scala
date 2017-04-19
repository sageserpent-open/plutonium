package com.sageserpent.plutonium

import scala.collection.JavaConverters._

/**
  * Created by Gerard on 09/07/2015.
  */
trait Scope extends javaApi.Scope {
  val nextRevision: World.Revision

  def renderAsIterable[Raw](
      bitemporal: Bitemporal[Raw]): java.lang.Iterable[Raw] =
    render(bitemporal).asJava
}
