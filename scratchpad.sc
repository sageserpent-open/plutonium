import org.scalacheck._
import org.scalacheck.Prop.forAllNoShrink
import org.scalacheck.Gen


val numbers = Gen.frequency(1 -> Gen.choose(0, 20), 2 -> Gen.choose(-50, -40))

val upperTrianglePasses = for {column <- numbers
                               row <- Gen.choose(column, 20)} yield column -> row

val property = forAllNoShrink(upperTrianglePasses) { case (column, row) =>
  println(column + ", " + row)
  column <= row
}

property.check
def curried(x: Int)(y: String) = x + y
val c1 = curried(2) _
val c2 = curried (_: Int) ("Hello")
c1("Hi")
c2(3)

val x, y = 2

x
y