import scala.spores._

trait Foo
{
  def render[Raw](): Iterable[Raw] = ???
}

object Bar {
  def apply[Raw](id: Int) = {
    spore {
      (foo: Foo) => {
        val raws = foo.render()
        val raw = raws.head
        val raw2 = foo.render().head  // Huh? - I see "the invocation of 'foo.render[Nothing]' is not static" here.
      }
    }
  }
}


