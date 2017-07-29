package com.sageserpent.plutonium

import scala.collection.JavaConverters._

trait Scope extends javaApi.Scope {
  val nextRevision: World.Revision

  def renderAsIterable[Item](
      bitemporal: Bitemporal[Item]): java.lang.Iterable[Item] =
    render(bitemporal).asJava
}
