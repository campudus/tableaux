
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
        "stringAttribute" -> "stringAttribute",
        "numberAttribute" -> 42,
        "objectAttribute" -> Json.obj(
          "someAttribute" -> "someString",
          "someOtherAttribute" -> 2.5
          ),
        "arrayAttribute" -> Json.arr("stringVal", 42, Json.obj("someAttribute" -> "someValue"))
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
  def createRegularTableWithInvalidAttributesWhichShouldFail(implicit c: TestContext): Unit = {
    exceptionTest("error.request.json.wrongtype") {
      val invalidAttributes = "notAValidJsonObject"
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "attributes" -> invalidAttributes
      )

      for {
        tablePost <- sendRequest("POST", "/tables", createTableJson)
      } yield  ()
    }
  }
}
