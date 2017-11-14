package unit

import models.forms.AirportQueryForm
import org.scalatestplus.play.PlaySpec

/**
 * Unit tests that do not require a running Play application.
 *
 * This is useful for testing forms and constraints.
 */
class UnitSpec extends PlaySpec {

  "AirportQueryForm" must {

    "apply successfully from map" in {

      val data = Map("criteria" -> "Greece", "searchBy" -> "byName")

      val boundForm = AirportQueryForm.form.bind(data)

      val widgetData = boundForm.value.get

      widgetData.criteria must equal("Greece")
      widgetData.searchBy must equal("byName")
    }

  }
}
