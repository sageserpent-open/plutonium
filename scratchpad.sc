

import de.sciss.fingertree.RangedSeq

type Pair = (Int, Int)

val rangedSeq = RangedSeq((1, 2), (-1, 5), (7, 10))(identity, implicitly[Ordering[Int]])



rangedSeq.intersect(2).toList

rangedSeq.filterIncludes((2, 2)).toList

rangedSeq.filterOverlaps((2, 2)).toList

rangedSeq.includes(2)

rangedSeq.interval

val biggerRangedSeq = rangedSeq + (1, 45)

biggerRangedSeq.interval

biggerRangedSeq.intersect(2).toList

biggerRangedSeq.filterIncludes((-1, -1)).toList

biggerRangedSeq.filterOverlaps((2, 2)).toList

biggerRangedSeq.includes(2)

biggerRangedSeq - (1 -> 3)

biggerRangedSeq - (1 -> 2)

biggerRangedSeq - (1, 47)


import scalaz.State
import scalaz.StateT.stateMonad
import scalaz.syntax.monad._

implicit val s = stateMonad[String]

import s.{put, get}

val stuff: State[String, String] = for {
  x <- 1.pure
  y <- 2.pure
  _ <- put("Wiggies")
  result <- get
} yield "Hello"

stuff.run("")