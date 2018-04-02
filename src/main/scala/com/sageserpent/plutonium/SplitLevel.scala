package com.sageserpent.plutonium

object Split {
  implicit def ordering[X: Ordering]: Ordering[Split[X]] =
    Ordering.by[Split[X], (X, SplitLevel)] {
      case Split(unlifted, splitLevel) => unlifted -> splitLevel
    }

  def lowerBoundOf[X: Ordering](unlifted: X): Split[X] =
    Split(unlifted, LowerBound)

  def alignedWith[X: Ordering](unlifted: X): Split[X] = Split(unlifted, Aligned)

  def upperBoundOf[X: Ordering](unlifted: X): Split[X] =
    Split(unlifted, UpperBound)
}

case class Split[X: Ordering](unlifted: X, splitLevel: SplitLevel)

object SplitLevel {
  implicit val ordering: Ordering[SplitLevel] = {
    case (LowerBound, LowerBound) => 0
    case (LowerBound, _)          => -1
    case (Aligned, LowerBound)    => 1
    case (Aligned, Aligned)       => 0
    case (Aligned, UpperBound)    => -1
    case (UpperBound, UpperBound) => 0
    case (UpperBound, _)          => 1
  }
}

sealed trait SplitLevel

case object LowerBound extends SplitLevel

case object Aligned extends SplitLevel

case object UpperBound extends SplitLevel
