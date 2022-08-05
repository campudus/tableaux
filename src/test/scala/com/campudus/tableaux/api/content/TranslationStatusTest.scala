package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class TranslationStatusTest extends TableauxTestBase {

  @Test
  def retrieveTranslationForOneTableWithFourRows(implicit c: TestContext): Unit = {
    okTest {
      val createMultilanguageColumn = Json.obj(
        "columns" ->
          Json.arr(
            Json.obj("kind" -> "text", "name" -> "Test Column 1", "multilanguage" -> true, "identifier" -> true)
          )
      )

      for {
        _ <- sendRequest("POST", s"/system/settings/langtags", Json.obj("value" -> Json.arr("fr", "es", "en")))

        tableId <- sendRequest("POST", "/tables", Json.obj("name" -> "Test")) map (_.getLong("id"))
        _ <- sendRequest("POST", s"/tables/$tableId/columns", createMultilanguageColumn)

        // empty row 1
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId1 = result.getLong("id")

        // empty row 2
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId2 = result.getLong("id")

        // empty row 3
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId3 = result.getLong("id")

        // empty row 4
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId4 = result.getLong("id")

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId1/annotations",
          Json.obj("langtags" -> Json.arr("fr", "es", "en"), "type" -> "flag", "value" -> "needs_translation")
        )

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId2/annotations",
          Json.obj("langtags" -> Json.arr("fr", "es"), "type" -> "flag", "value" -> "needs_translation")
        )

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/1/rows/$rowId3/annotations",
          Json.obj("langtags" -> Json.arr("fr"), "type" -> "flag", "value" -> "needs_translation")
        )

        translationStatus <- sendRequest("GET", s"/tables/translationStatus")
      } yield {
        val expectedTranslationStatus = Json.obj(
          "tables" -> Json.arr(
            Json.obj(
              "id" -> tableId.toInt,
              "translationStatus" -> Json.obj(
                "en" -> 0.75,
                "es" -> 0.5,
                "fr" -> 0.25
              )
            )
          ),
          "translationStatus" -> Json.obj(
            "en" -> 0.75,
            "es" -> 0.5,
            "fr" -> 0.25
          )
        )

        assertJSONEquals(expectedTranslationStatus, translationStatus)
      }
    }
  }

  @Test
  def retrieveTranslationForTwoTableWithFourRowsEach(implicit c: TestContext): Unit = {
    okTest {
      val createMultilanguageColumn = Json.obj(
        "columns" ->
          Json.arr(
            Json.obj("kind" -> "text", "name" -> "Test Column 1", "multilanguage" -> true, "identifier" -> true)
          )
      )

      for {
        _ <- sendRequest("POST", s"/system/settings/langtags", Json.obj("value" -> Json.arr("fr", "es", "en")))

        tableId1 <- sendRequest("POST", "/tables", Json.obj("name" -> "Test1")) map (_.getLong("id"))
        _ <- sendRequest("POST", s"/tables/$tableId1/columns", createMultilanguageColumn)

        tableId2 <- sendRequest("POST", "/tables", Json.obj("name" -> "Test2")) map (_.getLong("id"))
        _ <- sendRequest("POST", s"/tables/$tableId2/columns", createMultilanguageColumn)

        // empty row 1
        result <- sendRequest("POST", s"/tables/$tableId1/rows")
        rowId1 = result.getLong("id")

        // empty row 2
        result <- sendRequest("POST", s"/tables/$tableId1/rows")
        rowId2 = result.getLong("id")

        // empty row 3
        result <- sendRequest("POST", s"/tables/$tableId1/rows")
        rowId3 = result.getLong("id")

        // empty row 4
        result <- sendRequest("POST", s"/tables/$tableId1/rows")
        rowId4 = result.getLong("id")

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId1/columns/1/rows/$rowId1/annotations",
          Json.obj("langtags" -> Json.arr("fr", "es", "en"), "type" -> "flag", "value" -> "needs_translation")
        )

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId1/columns/1/rows/$rowId2/annotations",
          Json.obj("langtags" -> Json.arr("fr", "es"), "type" -> "flag", "value" -> "needs_translation")
        )

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId1/columns/1/rows/$rowId3/annotations",
          Json.obj("langtags" -> Json.arr("fr"), "type" -> "flag", "value" -> "needs_translation")
        )

        // empty row 1
        result <- sendRequest("POST", s"/tables/$tableId2/rows")

        // empty row 2
        result <- sendRequest("POST", s"/tables/$tableId2/rows")

        // empty row 3
        result <- sendRequest("POST", s"/tables/$tableId2/rows")

        // empty row 4
        result <- sendRequest("POST", s"/tables/$tableId2/rows")

        translationStatus <- sendRequest("GET", s"/tables/translationStatus")
      } yield {
        val expectedTranslationStatus = Json.obj(
          "tables" -> Json.arr(
            Json.obj(
              "id" -> tableId1.toInt,
              "translationStatus" -> Json.obj(
                "en" -> 0.75,
                "es" -> 0.5,
                "fr" -> 0.25
              )
            ),
            Json.obj(
              "id" -> tableId2.toInt,
              "translationStatus" -> Json.obj(
                "en" -> 1.0,
                "es" -> 1.0,
                "fr" -> 1.0
              )
            )
          ),
          "translationStatus" -> Json.obj(
            "en" -> (0.75 + 1) / 2.0,
            "es" -> (0.5 + 1) / 2.0,
            "fr" -> (0.25 + 1) / 2.0
          )
        )

        assertJSONEquals(expectedTranslationStatus, translationStatus)
      }
    }
  }
}
