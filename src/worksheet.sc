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

