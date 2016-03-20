import com.sageserpent.plutonium.{Bitemporal, Identified}

object Uranium {
  val fissileUraniumMassNumbers = Set(233, 235)
  val symbol = "U"
}

class Drum(val id: String, var mass: Double, var chemicalAssay: Bitemporal[ChemicalAssay]) extends Identified {
  type Id = String

  def isotopeMasses: Bitemporal[Map[(String, Int), Double]] = for {
    chemicalAssay <- chemicalAssay if true // The 'if' is just there to test support for for-comprehension filtering.
    isotopeMolarFractions <- chemicalAssay.isotopeMolarFractions
  } yield isotopeMolarFractions.mapValues(mass * _)

  def fissileUraniumMass: Bitemporal[Option[Double]] = for (isotopeMasses <- isotopeMasses) yield isotopeMasses.collectFirst { case ((Uranium.symbol, massNumber), fissileMass) if Uranium.fissileUraniumMassNumbers contains massNumber => fissileMass }
}

class ChemicalAssay(val id: String, var elementMolarFractions: Bitemporal[Map[String, Double]], var isotopicAssay: Bitemporal[IsotopicAssay]) {
  type Id = String

  def isotopeMolarFractions: Bitemporal[Map[(String, Int), Double]] = for {
    isotopicAssay <- isotopicAssay
    elementMolarFractions <- elementMolarFractions
  } yield elementMolarFractions.flatMap { case (element, elementMolarFraction) => isotopicAssay.isotopeMolarFractionsByElement(element).map { case (massNumber, isotopeMolarFraction) => (element -> massNumber) -> elementMolarFraction * isotopeMolarFraction } }
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

class Contract(val id: Long, var party: String, var counterparty: String, var instrument: Bitemporal[Instrument], var volume: Double) extends Identified {
  type Id = Long

  def fairPrice = for (instrument <- instrument) yield volume * instrument.fairPrice
}

