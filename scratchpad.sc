import cats.free.Cofree
import cats.implicits._

val bornFree = Cofree.unfold[Option, Int](0){
  case 20 => None
  case x =>
    println(s"$x Ow!")
    Some(1 + x)
}



bornFree.toList
