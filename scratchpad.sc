import com.sageserpent.americium.Unbounded._
import com.sageserpent.americium.{Finite, NegativeInfinity, PositiveInfinity, Unbounded}


Finite(67) < Finite(78)

Finite(67) >= Finite(78)

Finite(67) != Finite(78)

Finite(67) == Finite(67)

NegativeInfinity[Int] < PositiveInfinity()

val stuff = Seq(Finite(67), NegativeInfinity[Int], PositiveInfinity[Int], Finite(-34))

stuff.sorted(Ordering.ordered[Unbounded[Int]]).toList
