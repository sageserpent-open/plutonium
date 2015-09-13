import java.time.Instant

import com.sageserpent.infrastructure.{Unbounded, Finite, NegativeInfinity}
import org.scalacheck._
import org.scalacheck.Prop.forAllNoShrink
import org.scalacheck.Gen
import org.scalatest.path

import scala.reflect.runtime.universe._

import scala.collection.mutable
import scalaz.std.list.listInstance

import listInstance.foldableSyntax._

import scala.reflect.runtime._
import scala.reflect.runtime.universe._

import scala.collection.immutable.{Bag, SortedBagConfiguration, TreeBag}

import scala.collection.Searching._


def makeInstanceFromId[X: TypeTag](id: Any) = {
  val typeTag = implicitly[TypeTag[X]]
  val typeThingie = typeTag.tpe
  val constructor = typeThingie.decls.find(_.isConstructor) get
  val classMirror = currentMirror.reflectClass(typeThingie.typeSymbol.asClass)
  val constructorFunction = classMirror.reflectConstructor(constructor.asMethod)
  constructorFunction(id)
}

makeInstanceFromId[Tuple1[Int]](2)


implicit val tupleOrdering = new Ordering[(Unbounded[Instant], String)]{
  override def compare(x: (Unbounded[Instant], String), y: (Unbounded[Instant], String)): Int = x._1.compareTo(y._1)
}

implicit val config = SortedBagConfiguration.keepAll[(Unbounded[Instant], String)]

val emptyBag = TreeBag.empty[(Unbounded[Instant], String)]

val wayBack = NegativeInfinity[Instant]

val later = Finite(Instant.ofEpochSecond(664667266L))

val bag1 = emptyBag ++ List[(Unbounded[Instant], String)](wayBack -> "A", later -> "B")

val bag2 = bag1 ++ List[(Unbounded[Instant], String)](wayBack -> "C", later -> "D")

val bag3 = bag2 ++ List[(Unbounded[Instant], String)](later -> "F", wayBack -> "E")

val comprehensionOfTuples = for (tp <- bag3) yield tp

val shouldBeTwoOfThem = bag3 take 2

val shouldBeOneOfThem = bag3 take 1


/*(0 to 10 toList) search -1 insertionPoint
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
y*/