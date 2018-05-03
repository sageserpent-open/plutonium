
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

