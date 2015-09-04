import java.time.Instant

import org.scalacheck._
import org.scalacheck.Prop.forAllNoShrink
import org.scalacheck.Gen
import org.scalatest.path

import scala.collection.mutable
import scalaz.std.list.listInstance

import listInstance.foldableSyntax._

import scala.reflect.runtime.universe._

import scala.collection.mutable.TreeBag

implicit val config = mutable.SortedBagConfiguration.compact[Int]

val tb = TreeBag.empty[Int]

tb.add(1, 2)
tb.contents
tb.setMultiplicity(7, 6)
tb += 2
tb += 1
tb += 2
tb += 3

tb.contents
tb.size
tb

typeTag[Stream[String => Int]].tpe

val limits = 0 to (100, 5)

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