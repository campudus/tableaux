package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.{RequestCreation, TableauxTestBase}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonObject}

@RunWith(classOf[VertxUnitRunner])
class ColumnAttributesTest extends TableauxTestBase {
  def createTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.TextCol(name)).getJson
  def extractIdFromColumnAnswer(answer: JsonObject) = answer.getJsonArray("columns").getJsonObject(0).getInteger("id")
  val createTableJson = Json.obj("name" -> "Test Nr. 1")

  @Test
  def createColumnsWithoutAttributes(implicit c: TestContext): Unit = {
    okTest {
      val createColumn1 = createTextColumnJson("column1")
      val createColumn2 = createTextColumnJson("column2")

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
              "description" -> Json.obj()
            )
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))
        )
      )

      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj(
              "id" -> 2,
              "ordering" -> 2,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "attributes" -> Json.obj(),
              "description" -> Json.obj()
            )
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))
        )
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
        test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
      } yield {
        assertJSONEquals(expectedJson, test1)
        assertJSONEquals(expectedJson2, test2)
      }
    }
  }

  @Test
  def createColumnsWithValidAttributes(implicit c: TestContext): Unit = {
    okTest {
      val validAttributes = Json.obj(
        "stringAttribute" -> Json.obj(
          "type" -> "string",
          "value" -> "stringValue"
        ),
        "numberAttribute" -> Json.obj(
          "type" -> "number",
          "value" -> 42
        ),
        "booleanAttribute" -> Json.obj(
          "type" -> "boolean",
          "value" -> true
        ),
        "arrayAttribute" -> Json.obj(
          "type" -> "array",
          "value" -> Json.arr(Json.obj("type" -> "string", "value" -> "test"))
        )
      )
      val createColumn1 = Json.obj(
        "columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "column1", "attributes" -> validAttributes))
      )
      val createColumn2 = Json.obj(
        "columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "column2", "attributes" -> validAttributes))
      )

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
              "attributes" -> validAttributes,
              "description" -> Json.obj()
            )
            .mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))
        )
      )

      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json
            .obj(
              "id" -> 2,
              "ordering" -> 2,
              "multilanguage" -> false,
              "identifier" -> false,
              "displayName" -> Json.obj(),
              "attributes" -> validAttributes,
              "description" -> Json.obj()
            )
            .mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))
        )
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
        test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
      } yield {
        assertJSONEquals(expectedJson, test1)
        assertJSONEquals(expectedJson2, test2)
      }
    }
  }

  @Test
  def UpdateColumnsWithValidAttributes(implicit c: TestContext): Unit = {
    okTest {
      val attributes = Json.obj(
        "randomAttribute" -> Json.obj(
          "type" -> "string",
          "value" -> "stringValue"
        )
      )

      val createColumn1 =
        Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "column1")))
      val createColumn2 =
        Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "column2")))
      val updateColumn1 = Json.obj("attributes" -> attributes)
      val updateColumn2 = Json.obj("attributes" -> attributes)

      val expectedJson = Json.obj(
        "status" -> "ok",
        "attributes" -> attributes
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        created1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
        created2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
        updated1 <- sendRequest("POST", s"/tables/1/columns/${extractIdFromColumnAnswer(created1)}", updateColumn1)
        updated2 <- sendRequest("POST", s"/tables/1/columns/${extractIdFromColumnAnswer(created2)}", updateColumn2)
      } yield {
        assertJSONEquals(expectedJson, updated1)
        assertJSONEquals(expectedJson, updated2)
      }
    }
  }

  @Test
  def createColumnsWithInvalidAttributesType(implicit c: TestContext): Unit = {
    exceptionTest("error.request.json.wrongtype") {
      val InvalidAttributes = "InvalidAttributesValue"

      val createColumn1 = Json.obj(
        "columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "column1", "attributes" -> InvalidAttributes))
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
      } yield ()
    }
  }

  @Test
  def createColumnsWithInvalidAttributesContent(implicit c: TestContext): Unit = {
    exceptionTest("error.json.attributes") {
      val invalidAttributes = Json.obj(
        "stringAttribute" -> Json.obj(
          "type" -> "strig",
          "value" -> "stringValue"
        ),
        "numberAttribute" -> Json.obj(
          "type" -> "number",
          "value" -> "42"
        ),
        "booleanAttribute" -> Json.obj(
          "tpe" -> "boolean",
          "value" -> true
        ),
        "arrayAttribute" -> Json.obj(
          "type" -> "array",
          "vale" -> Json.arr(Json.obj("type" -> "string", "value" -> "test"))
        )
      )

      val createColumn1 = Json.obj(
        "columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "column1", "attributes" -> invalidAttributes))
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
      } yield ()
    }
  }

  @Test
  def UpdateColumnsWithInvalidAttributesType(implicit c: TestContext): Unit = {
    exceptionTest("error.request.json.wrongtype") {
      val invalidAttributes = "invalidAttributes"

      val createColumn1 = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "column1")))
      val updateColumn1 = Json.obj("attributes" -> invalidAttributes)

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        created1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
        updated1 <- sendRequest("POST", s"/tables/1/columns/${extractIdFromColumnAnswer(created1)}", updateColumn1)
      } yield ()
    }
  }

  @Test
  def UpdateColumnsWithInvalidAttributesContent(implicit c: TestContext): Unit = {
    exceptionTest("error.json.attributes") {

      val invalidAttributes = Json.obj(
        "stringAttribute" -> Json.obj(
          "type" -> "strig",
          "value" -> "stringValue"
        ),
        "numberAttribute" -> Json.obj(
          "type" -> "number",
          "value" -> "42"
        ),
        "booleanAttribute" -> Json.obj(
          "tpe" -> "boolean",
          "value" -> true
        ),
        "arrayAttribute" -> Json.obj(
          "type" -> "array",
          "vale" -> Json.arr(Json.obj("type" -> "string", "value" -> "test"))
        )
      )

      val createColumn1 = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "column1")))
      val updateColumn1 = Json.obj("attributes" -> invalidAttributes)

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        created1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
        updated1 <- sendRequest("POST", s"/tables/1/columns/${extractIdFromColumnAnswer(created1)}", updateColumn1)
      } yield ()
    }
  }
}
