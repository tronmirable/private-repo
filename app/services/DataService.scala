package services

import javax.inject._

import models.dto.{Country, Report}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import play.api.Configuration
import play.api.inject.ApplicationLifecycle
import services.functions.Translator._

import scala.concurrent.Future

@Singleton
class DataService @Inject()(config: Configuration, lifecycle: ApplicationLifecycle) {

  private val sparkMaster = config.getOptional[String]("spark.master").getOrElse("local[4]")

  private implicit val spark: SparkSession = SparkSession.builder
    .master(sparkMaster)
    .getOrCreate()

  import spark.implicits._

  private val countriesDF = EntityManager.read("schema/countries.json")
  private val airportsDF = EntityManager.read("schema/airports.json")
  private val runwaysDF = EntityManager.read("schema/runways.json")

  private val airports = airportsDF
    .withColumnRenamed("id", "airport_id")
    .withColumnRenamed("name", "airport_name")
    .withColumnRenamed("continent", "airport_continent")
    .withColumnRenamed("wikipedia_link", "airport_wikipedia_link")
    .withColumnRenamed("keywords", "airport_keywords")

  private val runways = runwaysDF
    .withColumnRenamed("id", "runway_id")

  def getByCountryName(countryName: String): List[Country] = {

    val searchCriteria = s"%${countryName.toLowerCase}%"

    val searchResult = countriesDF
      .where(lower($"name") like searchCriteria)
      .join(airports, $"code" === $"iso_country")
      .join(runways, $"airport_id" === $"airport_ref", "leftouter")
      .withColumn("runway", to_json(struct($"runway_id", $"airport_ref", $"airport_ident", $"length_ft", $"width_ft", $"surface", $"lighted", $"closed", $"le_ident", $"le_latitude_deg", $"le_longitude_deg", $"le_elevation_ft", $"le_heading_degT", $"le_displaced_threshold_ft", $"he_ident", $"he_latitude_deg", $"he_longitude_deg", $"he_elevation_ft", $"he_heading_degT", $"he_displaced_threshold_ft")))
      .groupBy($"id", $"code", $"name", $"continent", $"wikipedia_link", $"keywords", $"airport_id", $"ident", $"type", $"airport_name", $"latitude_deg", $"longitude_deg", $"elevation_ft", $"airport_continent", $"iso_country", $"iso_region", $"municipality", $"scheduled_service", $"gps_code", $"iata_code", $"local_code", $"home_link", $"airport_wikipedia_link", $"airport_keywords")
      .agg(collect_list($"runway") as "runways")
      .withColumn("airport", to_json(struct($"airport_id", $"ident", $"type", $"airport_name", $"latitude_deg", $"longitude_deg", $"elevation_ft", $"airport_continent", $"iso_country", $"iso_region", $"municipality", $"scheduled_service", $"gps_code", $"iata_code", $"local_code", $"home_link", $"airport_wikipedia_link", $"airport_keywords", $"runways")))
      .groupBy($"id", $"code", $"name", $"continent", $"wikipedia_link", $"keywords")
      .agg(collect_list($"airport") as "airports")

    searchResult.collect().par.map(translateToCountry).toList
  }

  def getByCountryCode(countryCode: String): List[Country] = {

    val searchCriteria = s"%${countryCode.toLowerCase}%"

    val searchResult = countriesDF
      .where(lower($"code") like searchCriteria)
      .join(airports, $"code" === $"iso_country")
      .join(runways, $"airport_id" === $"airport_ref", "leftouter")
      .withColumn("runway", to_json(struct($"runway_id", $"airport_ref", $"airport_ident", $"length_ft", $"width_ft", $"surface", $"lighted", $"closed", $"le_ident", $"le_latitude_deg", $"le_longitude_deg", $"le_elevation_ft", $"le_heading_degT", $"le_displaced_threshold_ft", $"he_ident", $"he_latitude_deg", $"he_longitude_deg", $"he_elevation_ft", $"he_heading_degT", $"he_displaced_threshold_ft")))
      .groupBy($"id", $"code", $"name", $"continent", $"wikipedia_link", $"keywords", $"airport_id", $"ident", $"type", $"airport_name", $"latitude_deg", $"longitude_deg", $"elevation_ft", $"airport_continent", $"iso_country", $"iso_region", $"municipality", $"scheduled_service", $"gps_code", $"iata_code", $"local_code", $"home_link", $"airport_wikipedia_link", $"airport_keywords")
      .agg(collect_list($"runway") as "runways")
      .withColumn("airport", to_json(struct($"airport_id", $"ident", $"type", $"airport_name", $"latitude_deg", $"longitude_deg", $"elevation_ft", $"airport_continent", $"iso_country", $"iso_region", $"municipality", $"scheduled_service", $"gps_code", $"iata_code", $"local_code", $"home_link", $"airport_wikipedia_link", $"airport_keywords", $"runways")))
      .groupBy($"id", $"code", $"name", $"continent", $"wikipedia_link", $"keywords")
      .agg(collect_list($"airport") as "airports")

    searchResult.collect().par.map(translateToCountry).toList
  }

  def generateReport(): Report = {

    val countriesAndAirports = countriesDF
      .join(airports, $"code" === $"iso_country")
      .cache()

    // Top 10 countries with highest/lowest airports number
    val topByAirportsRes = countriesAndAirports
      .groupBy($"name")
      .agg(count($"airport_id") as "airport_count")
      .cache()

    val highest = topByAirportsRes.orderBy(desc("airport_count")).limit(10).map(translateToCountriesWithAirportCount)
    val lowest = topByAirportsRes.orderBy(asc("airport_count")).limit(10).map(translateToCountriesWithAirportCount)

    val highestList = highest.collect().toList.sortBy(-_._2)
    val lowestList = lowest.collect().toList.sortBy(_._2)

    // Runways types (surfaces) per country
    val surfaceRes = countriesAndAirports
      .join(runways, $"airport_id" === $"airport_ref")
      .where($"surface".isNotNull)
      .groupBy($"name", $"surface")
      .agg(count($"surface") as "surface_type_count")
      .groupBy($"name")
      .agg(collect_list(map($"surface", $"surface_type_count")) as "surface_with_count")
      .map(translateToCountriesWithSurfaceTypes)

    val runwayPerCountry = surfaceRes.collect().toList

    // Top 10 runways identifiers
    val runwayIdentsRes = runwaysDF
      .groupBy($"le_ident")
      .agg(count($"le_ident") as "ident_count")
      .orderBy(desc("ident_count"))
      .limit(10)
      .map(translateToRunwayIdents)

    val top10Idents = runwayIdentsRes.collect().toList.sortBy(-_._2)

    // Clean-up cache
    countriesAndAirports.unpersist()
    topByAirportsRes.unpersist()

    Report(
      highestList,
      lowestList,
      runwayPerCountry,
      top10Idents
    )
  }

  // Clean-up cache
  lifecycle.addStopHook { () =>
    Future.successful {
      // Do clean-up stuff here
    }
  }
}
