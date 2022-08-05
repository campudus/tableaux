package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class TableLangtagsTest extends TableauxTestBase {

  @Test
  def updateRegularTableWithLangtags(implicit c: TestContext): Unit = {
    okTest {
      val createTableJson = Json.obj("name" -> "Table without langtags")
      val updateTableJson = Json.obj("name" -> "Table with langtags", "langtags" -> Json.arr("de-DE", "en-GB"))

      val expectedTableJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "hidden" -> false,
        "displayName" -> Json.obj(),
        "description" -> Json.obj()
      )

      for {
        langtags <- sendRequest("GET", "/system/settings/langtags").map(j => j.getJsonArray("value", Json.emptyArr()))

        tablePost <- sendRequest("POST", "/tables", createTableJson)
        tableId = tablePost.getLong("id").toLong

        tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)

        tableGet <- sendRequest("GET", s"/tables/$tableId")
      } yield {
        assertJSONEquals(
          expectedTableJson.copy().mergeIn(createTableJson.copy.mergeIn(Json.obj("langtags" -> langtags))),
          tablePost
        )
        assertJSONEquals(expectedTableJson.copy().mergeIn(updateTableJson), tableUpdate)
        assertJSONEquals(tableGet, tableUpdate)
      }
    }
  }

  @Test
  def updateTableWithLangtags(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Table with one langtag", "langtags" -> Json.arr("de-DE"))
    val updateTableJson = Json.obj("name" -> "Table with two langtags", "langtags" -> Json.arr("de-DE", "en-GB"))

    val expectedTableJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj()
    )

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong

      tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)

      tableGet <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertJSONEquals(expectedTableJson.copy().mergeIn(createTableJson), tablePost)
      assertJSONEquals(expectedTableJson.copy().mergeIn(updateTableJson), tableUpdate)
      assertJSONEquals(tableGet, tableUpdate)
    }
  }

  @Test
  def updateTableWithLangtagsEmpty(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Table with one langtag", "langtags" -> Json.arr("de-DE"))
    val updateTableJson = Json.obj("name" -> "Table with explicitly no langtags", "langtags" -> Json.arr())

    val expectedTableJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj()
    )

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong

      tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)

      tableGet <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertJSONEquals(expectedTableJson.copy().mergeIn(createTableJson), tablePost)
      assertJSONEquals(expectedTableJson.copy().mergeIn(updateTableJson), tableUpdate)
      assertJSONEquals(tableGet, tableUpdate)
    }
  }

  @Test
  def updateTableWithLangtagsNull(implicit c: TestContext): Unit = {
    okTest {
      val createTableJson = Json.obj("name" -> "Table with one langtag", "langtags" -> Json.arr("de-DE"))
      val updateTableJson = Json.obj("name" -> "Table with no langtags", "langtags" -> null)

      val expectedTableJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "hidden" -> false,
        "displayName" -> Json.obj(),
        "description" -> Json.obj()
      )

      for {
        langtags <- sendRequest("GET", "/system/settings/langtags").map(j => j.getJsonArray("value", Json.emptyArr()))

        tablePost <- sendRequest("POST", "/tables", createTableJson)
        tableId = tablePost.getLong("id").toLong

        tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)

        tableGet <- sendRequest("GET", s"/tables/$tableId")
      } yield {
        assertJSONEquals(expectedTableJson.copy().mergeIn(createTableJson), tablePost)
        assertJSONEquals(
          expectedTableJson.copy().mergeIn(Json.obj("name" -> "Table with no langtags", "langtags" -> langtags)),
          tableUpdate
        )
        assertJSONEquals(tableGet, tableUpdate)
      }
    }
  }
}
