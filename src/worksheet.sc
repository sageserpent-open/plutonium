import com.sageserpent.plutonium.Identified

object Uranium {
  val fissileUraniumMassNumbers = Set(233, 235)
  val symbol = "U"
}

class Drum(val id: String, var mass: Double, var chemicalAssay: ChemicalAssay) extends Identified {
  type Id = String

  def isotopeMasses = chemicalAssay.isotopeMolarFractions.mapValues(mass * _)

  def fissileUraniumMass = isotopeMasses.collectFirst { case ((Uranium.symbol, massNumber), fissileMass) if Uranium.fissileUraniumMassNumbers contains massNumber => fissileMass }
}

class ChemicalAssay(val id: String, var elementMolarFractions: Map[String, Double], var isotopicAssay: IsotopicAssay) {
  type Id = String

  def isotopeMolarFractions = elementMolarFractions.flatMap { case (element, elementMolarFraction) => isotopicAssay.isotopeMolarFractionsByElement(element).map { case (massNumber, isotopeMolarFraction) => (element -> massNumber) -> elementMolarFraction * isotopeMolarFraction }}
}

class IsotopicAssay(val id: String, var isotopeMolarFractionsByElement: Map[String, Map[Int, Double]]) extends Identified {
  type Id = String
}

// NOTE: abstract over what market data is required by specific instrument subclasses.
trait Instrument {
  this: Identified =>

  type Id = Long

  val id: Id

  def fairPrice: Double
}

class Contract(val id: Long, var party: String, var counterparty: String, var instrument: Instrument, var volume: Double) extends Identified {
  type Id = Long

  def fairPrice: Double = volume * instrument.fairPrice
}


