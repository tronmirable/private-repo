package models.dto

case class Report(top10CountriesHighestAirNum: List[(String, Long)],
                  top10CountriesLowestAirNum: List[(String, Long)],
                  runwaysTypePerCountry: List[(String, List[(String, Long)])],
                  top10MostCommonRunwaysIdent: List[(String, Long)])

object Report {

  def emptyReport(): Report = {
    Report(List(), List(), List(), List())
  }
}