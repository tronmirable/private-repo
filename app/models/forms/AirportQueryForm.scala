package models.forms

object AirportQueryForm {
  import play.api.data.Form
  import play.api.data.Forms._

  case class Data(criteria: String, searchBy: String)

  val form = Form(
    mapping(
      "criteria" -> text,
      "searchBy" -> text
    )(Data.apply)(Data.unapply)
  )
}
