package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class ColumnLanguageTypeTest extends TableauxTestBase {
  @Test
  def createColumnsWithLanguageTypeMultiLanguage(implicit c: TestContext): Unit = okTest {
    val postSimpleTable = Json.obj("name" -> "table1")
    val columnWithInvalidLanguageType = Json.obj(
      "name" -> "column1",
      "kind" -> "shorttext",
      "languageType" -> "language"
    )

    val postColumn1 = Json.obj("columns" -> Json.arr(columnWithInvalidLanguageType))

    for {
      table <- sendRequest("POST", "/tables", postSimpleTable)
      tableId = table.getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/columns", postColumn1)

      columnsJson <- sendRequest("GET", s"/tables/$tableId/columns")
    } yield {
      val column = columnsJson.getJsonArray("columns").getJsonObject(0)

      assertTrue(column.getBoolean("multilanguage"))
      assertEquals("language", column.getString("languageType"))
    }
  }

  @Test
  def createColumnsWithLanguageTypeMultiCountry(implicit c: TestContext): Unit = okTest {
    val postSimpleTable = Json.obj("name" -> "table1")
    val columnWithInvalidLanguageType = Json.obj(
      "name" -> "column1",
      "kind" -> "shorttext",
      "languageType" -> "country"
    )

    val postColumn1 = Json.obj("columns" -> Json.arr(columnWithInvalidLanguageType))

    for {
      table <- sendRequest("POST", "/tables", postSimpleTable)
      tableId = table.getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/columns", postColumn1)

      columnsJson <- sendRequest("GET", s"/tables/$tableId/columns")
    } yield {
      val column = columnsJson.getJsonArray("columns").getJsonObject(0)

      assertTrue(column.getBoolean("multilanguage"))
      assertEquals("country", column.getString("languageType"))
    }
  }

  @Test
  def createColumnsWithInvalidLanguageType(implicit c: TestContext): Unit = exceptionTest("error.json.languagetype") {
    val postSimpleTable = Json.obj("name" -> "table1")
    val columnWithInvalidLanguageType = Json.obj(
      "name" -> "column1",
      "kind" -> "shorttext",
      "languageType" -> "fuck"
    )

    val postColumn1 = Json.obj("columns" -> Json.arr(columnWithInvalidLanguageType))

    for {
      table <- sendRequest("POST", "/tables", postSimpleTable)
      tableId = table.getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/columns", postColumn1)
    } yield ()
  }
}
