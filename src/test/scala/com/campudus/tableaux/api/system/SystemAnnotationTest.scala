package com.campudus.tableaux.api.system

import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class SystemAnnotationTest extends TableauxTestBase {

  // GET /system/annotations

  @Test
  def retrieveAll(implicit c: TestContext): Unit = okTest {

    for {
      annotationConfigs <- sendRequest("GET", "/system/annotations")
    } yield {
      assertEquals(
        Json.arr(
          Json.obj(
            "name" -> "important",
            "priority" -> 1,
            "fgColor" -> "#ffffff",
            "bgColor" -> "#ff7474",
            "displayName" -> Json.obj(
              "de" -> "Wichtig",
              "en" -> "Important"
            ),
            "isMultilang" -> false,
            "isDashboard" -> true,
            "isCustom" -> false
          ),
          Json.obj(
            "name" -> "check-me",
            "priority" -> 2,
            "fgColor" -> "#ffffff",
            "bgColor" -> "#c274ff",
            "displayName" -> Json.obj(
              "de" -> "Bitte überprüfen",
              "en" -> "Please double-check"
            ),
            "isMultilang" -> false,
            "isDashboard" -> true,
            "isCustom" -> false
          ),
          Json.obj(
            "name" -> "postpone",
            "priority" -> 3,
            "fgColor" -> "#ffffff",
            "bgColor" -> "#999999",
            "displayName" -> Json.obj(
              "de" -> "Später",
              "en" -> "Later"
            ),
            "isMultilang" -> false,
            "isDashboard" -> true,
            "isCustom" -> false
          ),
          Json.obj(
            "name" -> "needs_translation",
            "priority" -> 4,
            "fgColor" -> "#ffffff",
            "bgColor" -> "#ffae74",
            "displayName" -> Json.obj(
              "de" -> "Übersetzung nötig",
              "en" -> "Translation necessary"
            ),
            "isMultilang" -> true,
            "isDashboard" -> true,
            "isCustom" -> false
          ),
        ),
        annotationConfigs.getJsonArray("annotations")
      )
    }
  }

}
