package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class TableAttributesTest extends TableauxTestBase {

  val baseExpectedTableJson = Json.obj("status" -> "ok",
                                       "id" -> 1,
                                       "name" -> "sometable",
                                       "displayName" -> Json.obj(),
                                       "description" -> Json.obj(),
                                       "hidden" -> false,
                                       "langtags" -> Json.arr("de-DE", "en-GB"))


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
      "arrayAttribute" ->  Json.obj(
        "type" -> "array",
        "value" -> Json.arr(Json.obj("type" -> "string", "value" -> "test"))
        )
      )

  @Test
  def createRegularTableWithoutAttributes(implicit c: TestContext): Unit = {
    okTest {
      val createTableJson = Json.obj(
        "name" -> "sometable"
      )
      val expectedTableJson = baseExpectedTableJson.mergeIn(Json.obj("attributes" -> Json.obj()))

      for {
        tablePost <- sendRequest("POST", "/tables", createTableJson)
        tableId = tablePost.getLong("id").toLong
        tableGet <- sendRequest("GET", s"/tables/$tableId")
      } yield {
        assertJSONEquals(expectedTableJson, tablePost)
        assertJSONEquals(expectedTableJson, tableGet)
      }
    }
  }

  @Test
  def createRegularTableWithValidAttributes(implicit c: TestContext): Unit = {
    okTest {
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "attributes" -> validAttributes
      )
      val expectedTableJson = baseExpectedTableJson.mergeIn(Json.obj("attributes" -> validAttributes))

      for {
        tablePost <- sendRequest("POST", "/tables", createTableJson)
        tableId = tablePost.getLong("id").toLong
        tableGet <- sendRequest("GET", s"/tables/$tableId")
      } yield {
        assertJSONEquals(expectedTableJson, tablePost)
        assertJSONEquals(expectedTableJson, tableGet)
      }
    }
  }

  @Test
  def createRegularTableWithInvalidAttributesTypeWhichShouldFail(implicit c: TestContext): Unit = {
    exceptionTest("error.request.json.wrongtype") {
      val invalidAttributes = "notAValidJsonObject"
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "attributes" -> invalidAttributes
      )

      for {
        tablePost <- sendRequest("POST", "/tables", createTableJson)
      } yield ()
    }
  }

  @Test
  def createRegularTableWithInvalidAttributesContentWhichShouldFail(implicit c: TestContext): Unit = {
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
      "arrayAttribute" ->  Json.obj(
        "type" -> "array",
        "vale" -> Json.arr(Json.obj("type" -> "string", "value" -> "test"))
        )
      )
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "attributes" -> invalidAttributes
      )

      for {
        tablePost <- sendRequest("POST", "/tables", createTableJson)
      } yield ()
    }
  }
}
