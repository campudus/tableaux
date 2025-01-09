package com.campudus.tableaux.api.system

import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.JSONCompareMode

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
          )
        ),
        annotationConfigs.getJsonArray("annotations")
      )
    }
  }

  // GET /system/annotations/<annotationName>

  @Test
  def retrieveSingleNonExisting(implicit c: TestContext): Unit =
    exceptionTest("NOT FOUND") {
      sendRequest("GET", "/system/annotations/does-not-exist")
    }

  @Test
  def retrieveSingle(implicit c: TestContext): Unit = okTest {
    for {
      annotationConfig <- sendRequest("GET", "/system/annotations/postpone")
    } yield {
      assertJSONEquals(
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
        annotationConfig
      )
    }
  }

  // DELETE /system/annotations/<annotationName>

  @Test
  def deleteSingleNonExisting(implicit c: TestContext): Unit =
    exceptionTest("NOT FOUND") {
      sendRequest("DELETE", "/system/annotations/does-not-exist")
    }

  @Test
  def deleteSingle(implicit c: TestContext): Unit = okTest {
    for {
      configsBeforeDeletion <- sendRequest("GET", "/system/annotations").map(_.getJsonArray("annotations"))

      deleteResult <- sendRequest("DELETE", "/system/annotations/postpone")

      configsAfterDeletion <- sendRequest("GET", "/system/annotations").map(_.getJsonArray("annotations"))
    } yield {
      assertEquals(4, configsBeforeDeletion.size)
      assertJSONEquals("""{  "status": "ok" }""", deleteResult.toString)
      assertEquals(3, configsAfterDeletion.size)
    }
  }

  // PATCH /system/annotations/<annotationName>

  @Test
  def updateSingleProperty(implicit c: TestContext): Unit = okTest {
    val configUpdate = Json.obj(
      "fgColor" -> "#dddddd"
    ).toString();

    for {
      configBeforeUpdate <- sendRequest("GET", "/system/annotations/postpone")
      configAfterUpdate <- sendRequest("PATCH", "/system/annotations/postpone", configUpdate)
    } yield {
      assertEquals("#ffffff", configBeforeUpdate.getString("fgColor"))
      assertEquals("#dddddd", configAfterUpdate.getString("fgColor"))
    }
  }

  @Test
  def updateInvalidProperty(implicit c: TestContext): Unit =
    exceptionTest("error.arguments") {
      for {
        _ <- sendRequest("PATCH", s"/system/annotations/postpone", """{ "foo": "bar" }""")
      } yield ()
    }

  @Test
  def updateAllProperties(implicit c: TestContext): Unit = okTest {
    val configUpdate = Json.obj(
      "priority" -> 5,
      "fgColor" -> "#dddddd",
      "bgColor" -> "#888888",
      "displayName" -> Json.obj(
        "de" -> "Jetzt",
        "en" -> "Now"
      ),
      "isMultilang" -> true,
      "isDashboard" -> false
    ).toString();

    for {
      result <- sendRequest("PATCH", "/system/annotations/postpone", configUpdate)
    } yield {
      assertJSONEquals(configUpdate, result.toString, JSONCompareMode.LENIENT)
    }
  }

  // POST /system/annotations

  @Test
  def createWithAllProperties(implicit c: TestContext): Unit = okTest {
    val configCreate = Json.obj(
      "name" -> "new_annotation",
      "priority" -> 5,
      "fgColor" -> "#aaaaaa",
      "bgColor" -> "#dddddd",
      "displayName" -> Json.obj(
        "de" -> "Neu",
        "en" -> "New"
      ),
      "isMultilang" -> true,
      "isDashboard" -> true
    ).toString()

    for {
      annotationConfig <- sendRequest("POST", "/system/annotations", configCreate)
    } yield {
      assertJSONEquals(configCreate, annotationConfig.toString, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def createWithOnlyMandatoryProperties(implicit c: TestContext): Unit = okTest {
    val configCreate = Json.obj(
      "name" -> "new_annotation",
      "priority" -> 5,
      "fgColor" -> "#aaaaaa",
      "bgColor" -> "#dddddd",
      "displayName" -> Json.obj(
        "de" -> "Neu",
        "en" -> "New"
      )
    ).toString()

    for {
      annotationConfig <- sendRequest("POST", "/system/annotations", configCreate)
    } yield {
      assertJSONEquals(configCreate, annotationConfig.toString, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def createWithInvalidName(implicit c: TestContext): Unit =
    exceptionTest("error.request.unique.cellAnnotationConfig") {
      for {
        _ <- sendRequest(
          "POST",
          "/system/annotations",
          Json.obj(
            "name" -> "postpone", // already exists
            "priority" -> 5,
            "fgColor" -> "#aaaaaa",
            "bgColor" -> "#dddddd",
            "displayName" -> Json.obj(
              "de" -> "Neu",
              "en" -> "New"
            )
          ).toString()
        )
      } yield ()
    }

  @Test
  def createWithMissingMandatoryProperties(implicit c: TestContext): Unit =
    exceptionTest("error.arguments") {
      sendRequest(
        "POST",
        "/system/annotations",
        Json.obj(
          "fgColor" -> "#aaaaaa",
          "bgColor" -> "#dddddd",
          "displayName" -> Json.obj(
            "de" -> "Neu",
            "en" -> "New"
          )
        ).toString()
      )
    }

}
