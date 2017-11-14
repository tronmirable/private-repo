package controllers

import javax.inject.Inject

import models.dto.Report
import models.forms.AirportQueryForm._
import play.api.Logger
import play.api.mvc._
import services.DataService

class ApplicationController @Inject()(cc: MessagesControllerComponents, dataService: DataService) extends MessagesAbstractController(cc) {

  def index = Action {
    Ok(views.html.index())
  }

  def listAirports(criteria: String, searchBy: String) = Action { implicit request: MessagesRequest[AnyContent] =>

    // Make a form from query variables to send it back to view to pre-fill search form
    val searchForm = form.bind(Map("criteria" -> criteria, "searchBy" -> searchBy))

    try {
      searchBy match {
        case "byName" if criteria.nonEmpty =>
          val filtered = dataService.getByCountryName(criteria)
          Ok(views.html.listAirports(filtered, searchForm, None))

        case "byCode" if criteria.nonEmpty =>
          val filtered = dataService.getByCountryCode(criteria)
          Ok(views.html.listAirports(filtered, searchForm, None))

        case _ =>
          Ok(views.html.listAirports(List(), searchForm, None))
      }
    } catch {
      case e: Exception =>
        Logger.error("An error occurred while searching", e)
        BadRequest(views.html.listAirports(List(), searchForm, Some("An error occurred while searching. See logs for details.")))
    }
  }

  def generateReport() = Action { implicit request: MessagesRequest[AnyContent] =>
    try {
      val report = dataService.generateReport()
      Ok(views.html.report(report, None))
    } catch {
      case e: Exception =>
        Logger.error("An error occurred while generating report", e)
        BadRequest(views.html.report(Report.emptyReport(), Some("An error occurred while generating report. See logs for details.")))
    }
  }

}