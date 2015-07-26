import org.scalacheck._
import org.scalacheck.Prop.forAllNoShrink
import org.scalacheck.Gen


val numbers = Gen.choose(0, 20)

val upperTrianglePasses = for {column <- numbers
                               row <- Gen.choose(column, 20)} yield column -> row

val property = forAllNoShrink(upperTrianglePasses) { case (column, row) =>
  println(column + ", " + row)
  column <= row
}

property.check

