package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.{RequestCreation, TableauxTestBase}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonObject, JsonArray}

@RunWith(classOf[VertxUnitRunner])
class StatusColumnTest extends TableauxTestBase {
  def extractIdFromColumnAnswer(answer: JsonObject) = answer.getJsonArray("columns").getJsonObject(0).getInteger("id")

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

  val createTableJson = Json.obj("name" -> "Test Nr. 1")

  @Test
  def createStatusColumnWithValidRules(implicit c: TestContext): Unit = {
    okTest {

      val firstConditionsObject = Json.obj(
        "composition" -> "AND",
        "values" -> Json.arr(
          Json.obj(
            "column" -> 1,
            "operator" -> "IS",
            "value" -> "some_short_text"
          ),
          Json.obj(
            "column" -> 2,
            "operator" -> "NOT",
            "value" -> "some_rich_text"
          ),
          Json.obj(
            "column" -> 3,
            "operator" -> "IS",
            "value" -> true
          ),
          Json.obj(
            "composition" -> "OR",
            "values" -> Json.arr(
              Json.obj(
                "column" -> 4,
                "operator" -> "IS",
                "value" -> 42
              ),
              Json.obj(
                "column" -> 4,
                "operator" -> "NOT",
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
            "value" -> "some_short_text"
          ),
          Json.obj(
            "column" -> 2,
            "operator" -> "NOT",
            "value" -> "some_rich_text"
          ),
          Json.obj(
            "column" -> 3,
            "operator" -> "IS",
            "value" -> true
          ),
          Json.obj(
            "composition" -> "AND",
            "values" -> Json.arr(
              Json.obj(
                "column" -> 4,
                "operator" -> "IS",
                "value" -> 42
              ),
              Json.obj(
                "column" -> 4,
                "operator" -> "NOT",
                "value" -> 420
              )
            )
          )
        )
      )

      val rulesJson = Json.arr(
        createSingleStatusRule("first", firstConditionsObject),
        createSingleStatusRule("second", secondConditionsObject)
      )

      val createColumn1 = createShortTextColumnJson("column1")
      val createColumn2 = createRichTextColumnJson("column2")
      val createColumn3 = createBooleanColumnJson("column3")
      val createColumn4 = createNumericColumnJson("column4")
      val createColumn5 = createStatusColumnJson("column5", rulesJson)

      val createOtherColumnsJson = Json.obj(
        "columns" -> Json.arr(
          createColumn1,
          createColumn2,
          createColumn3,
          createColumn4
        ))

      val createStatusColumn = Json.obj(
        "columns" -> Json.arr(
          createColumn5
        ))

      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 5,
                 "ordering" -> 5,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn5)
        )
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", createOtherColumnsJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createStatusColumn)
      } yield {
        assertJSONEquals(expectedJson, test1)
      }
    }
  }

  @Test
  def createStatusColumnWithoutRules(implicit c: TestContext): Unit = {
    okTest {

      val createColumn1 = Json.obj("name" -> "column1", "kind" -> "status")
      val createColumnsJson = Json.obj(
        "columns" -> Json.arr(
          createColumn1
        ))

      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj(
              "id" -> 1,
              "ordering" -> 1,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "attributes" -> Json.obj(),
              "rules" -> Json.arr(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn1)
        )
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumnsJson)
      } yield {
        assertJSONEquals(expectedJson, test1)
      }
    }
  }

  @Test
  def createStatusColumnWithEmptyRules(implicit c: TestContext): Unit = {
    okTest {

      val createColumn1 = createStatusColumnJson("column1", Json.arr())
      val createColumnsJson = Json.obj(
        "columns" -> Json.arr(
          createColumn1
        ))

      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj(
              "id" -> 1,
              "ordering" -> 1,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "attributes" -> Json.obj(),
              "rules" -> Json.arr(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn1)
        )
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumnsJson)
      } yield {
        assertJSONEquals(expectedJson, test1)
      }
    }
  }

  @Test
  def createStatusColumnWithStructurallyIncorrectRules(implicit c: TestContext): Unit = {
    exceptionTest("error.json.rules") {

      val firstConditionsObject = Json.obj(
        "composition" -> "AND",
        "values" -> Json.arr(
          "wrong_type_here",
          Json.obj(
            "column" -> 1,
            "opertor" -> "IS",
            "value" -> "some_short_text"
          ),
          Json.obj(
            "column" -> 2,
            "operator" -> "NOT",
            "value" -> "some_rich_text"
          ),
          Json.obj(
            "column" -> 3,
            "operator" -> "IS",
            "valu" -> true
          ),
          Json.obj(
            "compsition" -> "OR",
            "values" -> Json.arr(
              Json.obj(
                "column" -> 4,
                "operator" -> "IS",
                "value" -> 42
              ),
              Json.obj(
                "column" -> 4,
                "operator" -> "NOT",
                "value" -> 420
              )
            )
          )
        )
      )

      val rulesJson = Json.arr(
        createSingleStatusRule("first", firstConditionsObject)
      )

      val createColumn1 = createShortTextColumnJson("column1")
      val createColumn2 = createRichTextColumnJson("column2")
      val createColumn3 = createBooleanColumnJson("column3")
      val createColumn4 = createNumericColumnJson("column4")
      val createColumn5 = createStatusColumnJson("column5", rulesJson)

      val createOtherColumnsJson = Json.obj(
        "columns" -> Json.arr(
          createColumn1,
          createColumn2,
          createColumn3,
          createColumn4
        ))

      val createStatusColumn = Json.obj(
        "columns" -> Json.arr(
          createColumn5
        ))

      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj("id" -> 5,
                 "ordering" -> 5,
                 "multilanguage" -> false,
                 "identifier" -> false,
                 "displayName" -> Json.obj(),
                 "attributes" -> Json.obj(),
                 "description" -> Json.obj())
            .mergeIn(createColumn5)
        )
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", createOtherColumnsJson)
        _ <- sendRequest("POST", "/tables/1/columns", createStatusColumn)
      } yield ()
    }
  }

  @Test
  def createStatusColumnWithNonExistentColumn(implicit c: TestContext): Unit = {
    exceptionTest("NOT FOUND") {

      val firstConditionsObject = Json.obj(
        "composition" -> "AND",
        "values" -> Json.arr(
          Json.obj(
            "column" -> 1,
            "operator" -> "IS",
            "value" -> "some_short_text"
          ),
          Json.obj(
            "column" -> 2,
            "operator" -> "NOT",
            "value" -> "some_rich_text"
          ),
          Json.obj(
            "column" -> 3,
            "operator" -> "IS",
            "value" -> true
          ),
          Json.obj(
            "composition" -> "OR",
            "values" -> Json.arr(
              Json.obj(
                "column" -> 4,
                "operator" -> "IS",
                "value" -> 42
              ),
              Json.obj(
                "column" -> 42, //column does not exist
                "operator" -> "NOT",
                "value" -> 420
              )
            )
          )
        )
      )

      val rulesJson = Json.arr(
        createSingleStatusRule("first", firstConditionsObject)
      )

      val createColumn1 = createShortTextColumnJson("column1")
      val createColumn2 = createRichTextColumnJson("column2")
      val createColumn3 = createBooleanColumnJson("column3")
      val createColumn4 = createNumericColumnJson("column4")
      val createColumn5 = createStatusColumnJson("column5", rulesJson)

      val createColumnsJson = Json.obj(
        "columns" -> Json.arr(
          createColumn1,
          createColumn2,
          createColumn3,
          createColumn4,
          createColumn5
        ))

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumnsJson)
      } yield ()
    }
  }

  @Test
  def createStatusColumnWithInvalidColumnKind(implicit c: TestContext): Unit = {
    exceptionTest("error.request.column.wrongtype") {

      val firstConditionsObject = Json.obj(
        "composition" -> "AND",
        "values" -> Json.arr(
          Json.obj(
            "column" -> 1,
            "operator" -> "IS",
            "value" -> "some_short_text"
          ),
          Json.obj(
            "column" -> 2,
            "operator" -> "NOT",
            "value" -> "some_rich_text"
          ),
          Json.obj(
            "column" -> 3,
            "operator" -> "IS",
            "value" -> true
          ),
          Json.obj(
            "composition" -> "OR",
            "values" -> Json.arr(
              Json.obj(
                "column" -> 4,
                "operator" -> "IS",
                "value" -> 42
              ),
              Json.obj(
                "column" -> 4,
                "operator" -> "NOT",
                "value" -> 420
              )
            )
          )
        )
      )

      val rulesJson = Json.arr(
        createSingleStatusRule("first", firstConditionsObject)
      )

      val createColumn1 = RequestCreation.CurrencyCol("invalid_column").getJson
      val createColumn2 = createRichTextColumnJson("column2")
      val createColumn3 = createBooleanColumnJson("column3")
      val createColumn4 = createNumericColumnJson("column4")
      val createColumn5 = createStatusColumnJson("column5", rulesJson)

      val createOtherColumnsJson = Json.obj(
        "columns" -> Json.arr(
          createColumn1,
          createColumn2,
          createColumn3,
          createColumn4
        ))

      val createStatusColumn = Json.obj(
        "columns" -> Json.arr(
          createColumn5
        ))

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables", Json.obj("name" -> "table 2"))
        test1 <- sendRequest("POST", "/tables/1/columns", createOtherColumnsJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createStatusColumn)
      } yield ()
    }
  }

  @Test
  def createStatusColumnWithValueTypeNotMatchingColumnKind(implicit c: TestContext): Unit = {
    exceptionTest("error.request.status.value.wrongtype") {

      val firstConditionsObject = Json.obj(
        "composition" -> "AND",
        "values" -> Json.arr(
          Json.obj(
            "column" -> 1,
            "operator" -> "IS",
            "value" -> true //wrong value, should be string
          ),
          Json.obj(
            "column" -> 2,
            "operator" -> "NOT",
            "value" -> "some_rich_text"
          ),
          Json.obj(
            "column" -> 3,
            "operator" -> "IS",
            "value" -> true
          ),
          Json.obj(
            "composition" -> "OR",
            "values" -> Json.arr(
              Json.obj(
                "column" -> 4,
                "operator" -> "IS",
                "value" -> 42
              ),
              Json.obj(
                "column" -> 4,
                "operator" -> "NOT",
                "value" -> 420
              )
            )
          )
        )
      )

      val rulesJson = Json.arr(
        createSingleStatusRule("first", firstConditionsObject)
      )

      val createColumn1 = createShortTextColumnJson("column1")
      val createColumn2 = createRichTextColumnJson("column2")
      val createColumn3 = createBooleanColumnJson("column3")
      val createColumn4 = createNumericColumnJson("column4")
      val createColumn5 = createStatusColumnJson("column5", rulesJson)

      val createOtherColumnsJson = Json.obj(
        "columns" -> Json.arr(
          createColumn1,
          createColumn2,
          createColumn3,
          createColumn4
        ))

      val createStatusColumn = Json.obj(
        "columns" -> Json.arr(
          createColumn5
        ))

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createOtherColumnsJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createStatusColumn)
      } yield ()
    }
  }

  @Test
  def createStatusColumnWithMultiLanguageColumn(implicit c: TestContext): Unit = {
    exceptionTest("error.request.column.wrongtype") {

      val firstConditionsObject = Json.obj(
        "composition" -> "AND",
        "values" -> Json.arr(
          Json.obj(
            "column" -> 1,
            "operator" -> "IS",
            "value" -> "some_short_text"
          ),
          Json.obj(
            "column" -> 2,
            "operator" -> "NOT",
            "value" -> "some_rich_text"
          ),
          Json.obj(
            "column" -> 3,
            "operator" -> "IS",
            "value" -> true
          ),
          Json.obj(
            "composition" -> "OR",
            "values" -> Json.arr(
              Json.obj(
                "column" -> 4,
                "operator" -> "IS",
                "value" -> 42
              ),
              Json.obj(
                "column" -> 4,
                "operator" -> "NOT",
                "value" -> 420
              )
            )
          )
        )
      )

      val rulesJson = Json.arr(
        createSingleStatusRule("first", firstConditionsObject)
      )

      val createColumn1 = createShortTextColumnJson("column1").mergeIn(Json.obj("multilanguage" -> true))
      val createColumn2 = createRichTextColumnJson("column2")
      val createColumn3 = createBooleanColumnJson("column3")
      val createColumn4 = createNumericColumnJson("column4")
      val createColumn5 = createStatusColumnJson("column5", rulesJson)

      val createOtherColumnsJson = Json.obj(
        "columns" -> Json.arr(
          createColumn1,
          createColumn2,
          createColumn3,
          createColumn4
        ))

      val createStatusColumn = Json.obj(
        "columns" -> Json.arr(
          createColumn5
        ))

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createOtherColumnsJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createStatusColumn)
      } yield ()
    }
  }

  @Test
  def deleteStatusColum(implicit c: TestContext): Unit = {
    okTest {
      val firstConditionsObject = Json.obj(
        "composition" -> "AND",
        "values" -> Json.arr(
          Json.obj(
            "column" -> 1,
            "operator" -> "IS",
            "value" -> "some_short_text"
          ),
          Json.obj(
            "column" -> 2,
            "operator" -> "NOT",
            "value" -> "some_rich_text"
          ),
          Json.obj(
            "column" -> 3,
            "operator" -> "IS",
            "value" -> true
          ),
          Json.obj(
            "composition" -> "OR",
            "values" -> Json.arr(
              Json.obj(
                "column" -> 4,
                "operator" -> "IS",
                "value" -> 42
              ),
              Json.obj(
                "column" -> 4,
                "operator" -> "NOT",
                "value" -> 420
              )
            )
          )
        )
      )

      val rulesJson = Json.arr(
        createSingleStatusRule("first", firstConditionsObject)
      )

      val createColumn1 = createShortTextColumnJson("column1")
      val createColumn2 = createRichTextColumnJson("column2")
      val createColumn3 = createBooleanColumnJson("column3")
      val createColumn4 = createNumericColumnJson("column4")
      val createColumn5 = createStatusColumnJson("column5", rulesJson)

      val createOtherColumnsJson = Json.obj(
        "columns" -> Json.arr(
          createColumn1,
          createColumn2,
          createColumn3,
          createColumn4
        ))

      val createStatusColumn = Json.obj(
        "columns" -> Json.arr(
          createColumn5
        ))

      val expectedJson = Json.obj("status" -> "ok")

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", createOtherColumnsJson)
        _ <- sendRequest("POST", "/tables/1/columns", createStatusColumn)
        deleted <- sendRequest("DELETE", "/tables/1/columns/5", createStatusColumn)
      } yield {
        assertJSONEquals(expectedJson, deleted)
      }
    }
  }
}
