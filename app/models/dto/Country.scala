package models.dto

case class Country(id: Long,
                   code: String,
                   name: String,
                   continent: String,
                   wikipediaLink: Option[String],
                   keywords: Option[String],
                   airports: List[Airport])
