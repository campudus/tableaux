package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.JSONCompareMode

@RunWith(classOf[VertxUnitRunner])
class StatusValuesTest extends TableauxTestBase {

  val createTableJson: JsonObject = Json.obj("name" -> "Test Table 1")

  val createStringColumnJson: JsonObject =
    Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))

  val createNumberColumnJson: JsonObject =
    Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))

  val createBooleanColumnJson: JsonObject =
    Json.obj("columns" -> Json.arr(Json.obj("kind" -> "boolean", "name" -> "Test Column 3")))

  def createStatusColumnJson(name: String, rules: JsonArray) =
    Json.obj("name" -> name, "kind" -> "status", "rules" -> rules)
  def createShortTextColumnJson(name: String) = Json.obj("name" -> name, "kind" -> "shorttext")
  def createRichTextColumnJson(name: String) = Json.obj("name" -> name, "kind" -> "richtext")
  def createBooleanColumnJson(name: String) = Json.obj("name" -> name, "kind" -> "boolean")
  def createNumericColumnJson(name: String) = Json.obj("name" -> name, "kind" -> "numeric")

  def createSingleStatusRule(name: String, conditions: JsonObject) =
    Json
      .obj(
        "name" -> name,
        "displayName" -> Json.obj(
          "de" -> "german_name"
        ),
        "color" -> "#ffffff",
        "icon" -> Json.obj(
          "type" -> "fa",
          "value" -> "some_icon"
        ),
        "tooltip" -> Json.obj(
          "de" -> "tooltip_value"
        )
      )
      .mergeIn(Json.obj("conditions" -> conditions))

  @Test
  def retrieveStatusCell(implicit c: TestContext): Unit = {
    okTest {
      val expectedJson = Json.obj(
        "status" -> "ok",
        "value" -> Json.arr(
          false,
          true
        )
      )

      val firstConditionsObject = Json.obj(
        "composition" -> "AND",
        "values" -> Json.arr(
          Json.obj(
            "column" -> 1,
            "operator" -> "IS",
            "value" -> "short_text_value"
          ),
          Json.obj(
            "column" -> 2,
            "operator" -> "IS",
            "value" -> "wrong_text_value" // wrong value, should fail
          ),
          Json.obj(
            "column" -> 4,
            "operator" -> "IS",
            "value" -> true
          ),
          Json.obj(
            "composition" -> "OR",
            "values" -> Json.arr(
              Json.obj(
                "column" -> 3,
                "operator" -> "IS",
                "value" -> 42
              ),
              Json.obj(
                "column" -> 3,
                "operator" -> "IS",
                "value" -> 420
              )
            )
          )
        )
      )

      val secondConditionsObject = Json.obj(
        "composition" -> "OR",
        "values" -> Json.arr(
          Json.obj(
            "column" -> 1,
            "operator" -> "IS",
            "value" -> "wrong_short_text"
          ),
          Json.obj(
            "column" -> 2,
            "operator" -> "IS",
            "value" -> "wrong_rich_text"
          ),
          Json.obj(
            "column" -> 4,
            "operator" -> "IS",
            "value" -> false
          ),
          Json.obj(
            "composition" -> "AND",
            "values" -> Json.arr(
              Json.obj(
                "column" -> 3,
                "operator" -> "IS",
                "value" -> 42
              ),
              Json.obj(
                "column" -> 3,
                "operator" -> "NOT",
                "value" -> 420
              )
            )
          )
        )
      )
      val rules = Json.arr(
        createSingleStatusRule("first_rule", firstConditionsObject),
        createSingleStatusRule("second_rule", secondConditionsObject)
      )

      val createStatusColumn = Json.obj("columns" -> Json.arr(createStatusColumnJson("status_column", rules)))

      for {
        _ <- createFullStatusTestTable()
        _ <- sendRequest("POST", "/tables/1/columns", createStatusColumn)
        statusCellValue <- sendRequest("GET", "/tables/1/columns/5/rows/1")
      } yield {
        assertEquals(expectedJson, statusCellValue)
      }
    }
  }

}
