import com.sageserpent.plutonium.{Bitemporal, Identified}


object Uranium{
  val fissileUraniumMassNumbers = Set(233, 235)
  val symbol = "U"
}

class Drum(val id: String, val mass: Double, val chemicalAssay: Bitemporal[ChemicalAssay]) extends Identified {
  type Id = String

  def isotopeMasses = for {chemicalAssay <- chemicalAssay
                           isotopeMolarFractions <- chemicalAssay.isotopeMolarFractions} yield isotopeMolarFractions.mapValues(mass * _)

  def fissileUraniumMass = for (isotopeMasses <- isotopeMasses) yield isotopeMasses.collectFirst { case ((Uranium.symbol, massNumber), fissileMass) if Uranium.fissileUraniumMassNumbers contains massNumber => fissileMass }
}

class ChemicalAssay(val id: String, val elementMolarFractions: Bitemporal[Map[String, Double]], val isotopicAssay: Bitemporal[IsotopicAssay]){
  type Id = String

  def isotopeMolarFractions = for {isotopicAssay <- isotopicAssay
                                   elementMolarFractions <- elementMolarFractions} yield elementMolarFractions.flatMap { case (element, elementMolarFraction) => isotopicAssay.isotopeMolarFractionsByElement(element).map { case (massNumber, isotopeMolarFraction) => (element -> massNumber) -> elementMolarFraction * isotopeMolarFraction } }
}

class IsotopicAssay(val id: String, val isotopeMolarFractionsByElement: Map[String, Map[Int, Double]]) extends Identified {
  type Id = String
}

// NOTE: abstract over what market data is required by specific instrument subclasses.
trait Instrument
{
  this: Identified =>

  type Id = Long
  type Volume

  val id: Id

  def fairPrice: Double
  def fairPrice(volume: Volume)(implicit conversion: Volume => Double): Double
}

class Contract(val id: Long, val party: String, val counterparty: String, val instrument: Bitemporal[Instrument], val volume: Instrument#Volume) extends Identified {
  type Id = Long
}


// A question - what happens if a bitemporal computation uses 'Bitemporal.withId' to create a bitemporal value on the fly in a computation? This looks like a can of worms
// semantically! It isn't though - because the only way the id makes sense to the api / scope implementation is when the bitemporal result is rendered - and at that point, it is the
// world line of relevant events that determines what the id refers to. Here's another way of thinking about it ... shouldn't the two bits of code below doing the same thing?

class Example(val id: Int) extends Identified{
  type Id = Int

  // One way...

  private var anotherBitemporal: Bitemporal[Example] = Bitemporal.none

  // An event would call this.
  def referenceToAnotherBitemporal_=(id: Int) = {
    this.anotherBitemporal = Bitemporal.withId[Example](id)
  }

  def referenceToAnotherBitemporal_ = this.anotherBitemporal



  // Another way (ugh)...

  private var anotherBitemporalId: Option[Int] = None

  // An event would call this.
  def referenceToAnotherBitemporal2_=(id: Int) = {
    this.anotherBitemporalId = Some(id)
  }

  def referenceToAnotherBitemporal2_ = this.anotherBitemporalId match {
    case Some(id) => Bitemporal.withId[Example](id)
    case None => Bitemporal.none[Example]
  }
}