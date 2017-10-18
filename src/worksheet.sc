object Uranium {
  val fissileUraniumMassNumbers = Set(233, 235)
  val symbol                    = "U"
}

class Drum(val id: String, var mass: Double, var chemicalAssay: ChemicalAssay) {
  def isotopeMasses = chemicalAssay.isotopeMolarFractions.mapValues(mass * _)

  def fissileUraniumMass =
    isotopeMasses.collect {
      case ((Uranium.symbol, massNumber), fissileMass)
          if Uranium.fissileUraniumMassNumbers contains massNumber =>
        fissileMass
    } sum
}

class ChemicalAssay(val id: String,
                    var elementMolarFractions: Map[String, Double],
                    var isotopicAssay: IsotopicAssay) {
  def isotopeMolarFractions = elementMolarFractions.flatMap {
    case (element, elementMolarFraction) =>
      isotopicAssay.isotopeMolarFractionsByElement(element).map {
        case (massNumber, isotopeMolarFraction) =>
          (element -> massNumber) -> elementMolarFraction * isotopeMolarFraction
      }
  }
}

class IsotopicAssay(
    val id: String,
    var isotopeMolarFractionsByElement: Map[String, Map[Int, Double]]) {
}

// NOTE: abstract over what market data is required by specific instrument subclasses.
trait Instrument {
  val id: Long

  def fairPrice: Double
}

class Contract(val id: Long,
               var party: String,
               var counterparty: String,
               var instrument: Instrument,
               var volume: Double) {
  def fairPrice: Double = volume * instrument.fairPrice
}
