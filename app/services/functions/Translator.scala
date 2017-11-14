package services.functions

import models.dto.{Airport, Country, Runway}
import org.apache.spark.sql.Row
import play.api.libs.json.Reads._
import play.api.libs.json._

import scala.collection.mutable
import scala.util.Try

object Translator {

  def translateToCountry(row: Row): Country = {
    val id = row.getAs[Long]("id")
    val code = row.getAs[String]("code")
    val name = row.getAs[String]("name")
    val continent = row.getAs[String]("continent")
    val wikipediaLink = Try(row.getAs[String]("wikipedia_link")).toOption
    val keywords = Try(row.getAs[String]("keywords")).toOption

    val airports = row.getAs[mutable.WrappedArray[String]]("airports").par.flatMap(a => translateToAirport(Json.parse(a))).toList

    Country(id, code, name, continent, wikipediaLink, keywords, airports)
  }

  private def translateToAirport(row: JsValue): Option[Airport] = {

    if (row.as[JsObject].fields.isEmpty) return None

    val id = (row \ "airport_id").as[Long]
    val ident = (row \ "ident").as[String]
    val `type` = (row \ "type").as[String]
    val name = (row \ "airport_name").as[String]
    val latitudeDeg = (row \ "latitude_deg").as[BigDecimal]
    val longitudeDeg = (row \ "longitude_deg").as[BigDecimal]
    val elevationFt = (row \ "elevation_ft").asOpt[Long]
    val continent = (row \ "airport_continent").as[String]
    val isoCountry = (row \ "iso_country").as[String]
    val isoRegion = (row \ "iso_region").as[String]
    val municipality = (row \ "municipality").asOpt[String]
    val scheduledService = (row \ "scheduled_service").as[String]
    val gpsCode = (row \ "gps_code").asOpt[String]
    val iataCode = (row \ "iata_code").asOpt[String]
    val localCode = (row \ "local_code").asOpt[String]
    val homeLink = (row \ "home_link").asOpt[String]
    val wikipediaLink = (row \ "airport_wikipedia_link").asOpt[String]
    val keywords = (row \ "airport_keywords").asOpt[String]

    val runways = (row \ "runways").as[mutable.WrappedArray[String]].par.flatMap(r => translateToRunway(Json.parse(r))).toList

    Some(Airport(id, ident, `type`, name, latitudeDeg, longitudeDeg, elevationFt, continent, isoCountry, isoRegion, municipality, scheduledService, gpsCode, iataCode, localCode, homeLink, wikipediaLink, keywords, runways))
  }

  private def translateToRunway(row: JsValue): Option[Runway] = {

    if (row.as[JsObject].fields.isEmpty) return None

    val id = (row \ "runway_id").as[Long]
    val airportRef = (row \ "airport_ref").as[Long]
    val airportIdent = (row \ "airport_ident").as[String]
    val lengthFt = (row \ "length_ft").asOpt[String].map(_.toLong)
    val widthFt = (row \ "width_ft").asOpt[String].map(_.toLong)
    val surface = (row \ "surface").asOpt[String]
    val lighted = (row \ "lighted").asOpt[String].map(toBoolean)
    val closed =  (row \ "closed").asOpt[String].map(toBoolean)
    val leIdent = (row \ "le_ident").asOpt[String]
    val leLatitudeDeg = (row \ "le_latitude_deg").asOpt[BigDecimal]
    val leLongitudeDeg = (row \ "le_longitude_deg").asOpt[BigDecimal]
    val leElevationFt = (row \ "le_elevation_ft").asOpt[String].map(_.toLong)
    val leHeadingDegT = (row \ "le_heading_degT").asOpt[BigDecimal]
    val leDisplacedThresholdFt = (row \ "le_displaced_threshold_ft").asOpt[Long]
    val heIdent = (row \ "he_ident").asOpt[String]
    val heLatitudeDeg = (row \ "he_latitude_deg").asOpt[BigDecimal]
    val heLongitudeDeg = (row \ "he_longitude_deg").asOpt[BigDecimal]
    val heElevationFt = (row \ "he_elevation_ft").asOpt[String].map(_.toLong)
    val heHeadingDegT = (row \ "he_heading_degT").asOpt[BigDecimal]
    val heDisplacedThresholdFt = (row \ "he_displaced_threshold_ft").asOpt[String].map(_.toLong)

    Some(Runway(id, airportRef, airportIdent, lengthFt, widthFt, surface, lighted, closed, leIdent, leLatitudeDeg, leLongitudeDeg, leElevationFt, leHeadingDegT, leDisplacedThresholdFt, heIdent, heLatitudeDeg, heLongitudeDeg, heElevationFt, heHeadingDegT, heDisplacedThresholdFt))
  }

  private def toBoolean(x: String) = if (x == "0") false else true

  def translateToCountriesWithAirportCount(row: Row): (String, Long) = {
    val name = row.getAs[String]("name")
    val count = row.getAs[Long]("airport_count")

    (name, count)
  }

  def translateToCountriesWithSurfaceTypes(row: Row): (String, List[(String, Long)]) = {
    val name = row.getAs[String]("name")
    val runways = row.getAs[mutable.WrappedArray[Map[String, Long]]]("surface_with_count").flatten.toList

    (name, runways)
  }

  def translateToRunwayIdents(row: Row): (String, Long) = {
    val ident = row.getAs[String]("le_ident")
    val count = row.getAs[Long]("ident_count")

    (ident, count)
  }

}
