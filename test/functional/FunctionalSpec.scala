package functional

import controllers.ApplicationController
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.play.PlaySpec
import org.scalatestplus.play.guice.GuiceOneAppPerSuite
import play.api.http.Status
import play.api.test.CSRFTokenHelper._
import play.api.test.Helpers._
import play.api.test.{FakeRequest, _}
import services.DataService

/**
 * Functional specification that has a running Play application.
 *
 * This is good for testing filter functionality, such as CSRF token and template checks.
 *
 * See https://www.playframework.com/documentation/2.6.x/ScalaFunctionalTestingWithScalaTest for more details.
 */
class FunctionalSpec extends PlaySpec with GuiceOneAppPerSuite with Injecting with ScalaFutures {

  "DataService" must {

    "get country with airports by country name" in {

      val dataService = inject[DataService]

      val result = dataService.getByCountryName("Greece")

      result.length must equal(1)
      result.head.name must equal("Greece")
      result.head.airports.length must equal(73)
    }

    "get country with airports by country code" in {

      val dataService = inject[DataService]

      val result = dataService.getByCountryCode("GR")

      result.length must equal(1)
      result.head.name must equal("Greece")
      result.head.airports.length must equal(73)
    }

    "get countries with airports by country name part" in {

      val dataService = inject[DataService]

      val result = dataService.getByCountryName("bar")

      result.length must equal(3)
      result.exists(_.name == "Antigua and Barbuda") mustBe true
      result.exists(_.name == "Saint Barthélemy") mustBe true
      result.exists(_.name == "Barbados") mustBe true
    }

    "get countries with airports by country code part" in {

      val dataService = inject[DataService]

      val result = dataService.getByCountryCode("b")

      result.length must equal(23)
      result.exists(_.name == "Benin") mustBe true
    }

    "get empty result by nonexistent country name" in {

      val dataService = inject[DataService]

      val result = dataService.getByCountryName("Narnia")

      result.length must equal(0)
    }

    "get empty result by nonexistent country code" in {

      val dataService = inject[DataService]

      val result = dataService.getByCountryCode("ccc")

      result.length must equal(0)
    }

    "generate reports" in {

      val dataService = inject[DataService]

      val result = dataService.generateReport()

      result.top10CountriesHighestAirNum.length must equal(10)
      result.top10CountriesHighestAirNum.head must equal("United States" -> 21501)

      result.top10CountriesLowestAirNum.length must equal(10)

      val runwaysPerCountryTest = result.runwaysTypePerCountry.find(_._1 == "Chad")
      runwaysPerCountryTest.isDefined mustBe true
      runwaysPerCountryTest.get._2.contains("UNK" -> 1) mustBe true
      runwaysPerCountryTest.get._2.contains("ASP" -> 4) mustBe true

      result.top10MostCommonRunwaysIdent.length must equal(10)
      result.top10MostCommonRunwaysIdent.head must equal("H1" -> 5566)
    }
  }

  "ApplicationController" must {

    "process a search request successfully" in {
      // Pull the controller from the already running Play application, using Injecting
      val controller = inject[ApplicationController]

      // Call using the FakeRequest and CSRF token
      val request = FakeRequest().withCSRFToken
      val futureResult = controller.listAirports("Greece", "byName").apply(request)

      status(futureResult) must be(Status.OK)
      contentType(futureResult) must be(Some("text/html"))
    }

    "process a reports request successfully" in {
      // Pull the controller from the already running Play application, using Injecting
      val controller = inject[ApplicationController]

      // Call using the FakeRequest
      val request = FakeRequest()
      val futureResult = controller.generateReport().apply(request)

      status(futureResult) must be(Status.OK)
      contentType(futureResult) must be(Some("text/html"))
    }
  }

}
