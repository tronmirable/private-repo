package services

import com.google.gson.JsonParseException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.Logger
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._

import scala.util.Try

case class SchemaColumn(name: String, `type`: String)
case class SchemaConfig(path: String, columns: List[SchemaColumn])

object EntityManager extends SchemaJsonFormatter {

  private val environment = play.Environment.simple()

  def read(schemaLocation: String)(implicit spark: SparkSession): DataFrame = {
    val stream = environment.resourceAsStream(schemaLocation)
    val json = Json.parse(stream)

    val config = Json.fromJson[SchemaConfig](json).getOrElse(throw new JsonParseException(s"Unable to parse schema: $schemaLocation"))
    val schemaCols = config.columns.map(c => StructField(c.name, typeMappings(c.`type`)))
    val csvSchema = StructType(schemaCols)

    Logger.debug(s"Parsed schema $schemaLocation for file ${config.path}")

    val csvFilePath = environment.resource(config.path).getPath

    Try {
      spark.sqlContext.read
        .option("mode", "DROPMALFORMED")
        .schema(csvSchema)
        .csv(csvFilePath)
    } getOrElse {
      Logger.warn(s"No file was processed: $csvFilePath")
      spark.emptyDataFrame
    }
  }

  private val typeMappings = Map(
    "String" -> DataTypes.StringType,
    "Integer" -> DataTypes.IntegerType,
    "Long" -> DataTypes.LongType,
    "Double" -> DataTypes.DoubleType,
    "Boolean" -> DataTypes.BooleanType,
    "Date" -> DataTypes.DateType,
    "BigDecimal" -> DataTypes.createDecimalType(18, 6)
  )

}

trait SchemaJsonFormatter {

  implicit val schemaColumnReads: Reads[SchemaColumn] = (
    (JsPath \ "name").read[String] and
      (JsPath \ "type").read[String]
    ).apply(SchemaColumn.apply _)

  implicit val schemaConfigReadsBuilder: Reads[SchemaConfig] = (
    (JsPath \ "path").read[String] and
      (JsPath \ "columns").read[List[SchemaColumn]]
    ).apply(SchemaConfig.apply _)
}