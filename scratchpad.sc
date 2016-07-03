import rx.lang.scala._
import rx.lang.scala.schedulers._

import scala.concurrent.duration._


val foo = for {
  thing <- Observable.from(1 to 20)
  things <- Observable.interval(1 micros) map (thing -> _ * thing) take 5
} yield things

foo.toBlocking.toList