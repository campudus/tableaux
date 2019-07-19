package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Assert._
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
      assertFalse(column.containsKey("countryCodes"))
      assertEquals("language", column.getString("languageType"))
    }
  }

  @Test
  def createColumnsWithLanguageTypeNeutral(implicit c: TestContext): Unit = okTest {
    val postSimpleTable = Json.obj("name" -> "table1")
    val columnWithInvalidLanguageType = Json.obj(
      "name" -> "column1",
      "kind" -> "shorttext",
      "languageType" -> "neutral"
    )

    val postColumn1 = Json.obj("columns" -> Json.arr(columnWithInvalidLanguageType))

    for {
      table <- sendRequest("POST", "/tables", postSimpleTable)
      tableId = table.getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/columns", postColumn1)

      columnsJson <- sendRequest("GET", s"/tables/$tableId/columns")
    } yield {
      val column = columnsJson.getJsonArray("columns").getJsonObject(0)

      assertFalse(column.getBoolean("multilanguage"))
      assertFalse(column.containsKey("languageType"))
      assertFalse(column.containsKey("countryCodes"))
    }
  }

  @Test
  def createColumnsWithBackwardCompatibleLanguageTypeNeutral(implicit c: TestContext): Unit = okTest {
    val postSimpleTable = Json.obj("name" -> "table1")
    val columnWithInvalidLanguageType = Json.obj(
      "name" -> "column1",
      "kind" -> "shorttext",
      "multilanguage" -> false
    )

    val postColumn1 = Json.obj("columns" -> Json.arr(columnWithInvalidLanguageType))

    for {
      table <- sendRequest("POST", "/tables", postSimpleTable)
      tableId = table.getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/columns", postColumn1)

      columnsJson <- sendRequest("GET", s"/tables/$tableId/columns")
    } yield {
      val column = columnsJson.getJsonArray("columns").getJsonObject(0)

      assertFalse(column.getBoolean("multilanguage"))
      assertFalse(column.containsKey("languageType"))
      assertFalse(column.containsKey("countryCodes"))
    }
  }

  @Test
  def createColumnsWithLanguageTypeMultiCountryAndCountryCodes(implicit c: TestContext): Unit = okTest {
    val postSimpleTable = Json.obj("name" -> "table1")
    val columnWithInvalidLanguageType = Json.obj(
      "name" -> "column1",
      "kind" -> "currency",
      "languageType" -> "country",
      "countryCodes" -> Json.arr("DE", "AT", "GB")
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
      assertEquals(Json.arr("DE", "AT", "GB"), column.getJsonArray("countryCodes"))
    }
  }

  @Test
  def changeCountryCodes(implicit c: TestContext): Unit = okTest {
    val postSimpleTable = Json.obj("name" -> "table1")
    val columnWithInvalidLanguageType = Json.obj(
      "name" -> "column1",
      "kind" -> "shorttext",
      "languageType" -> "country",
      "countryCodes" -> Json.arr("DE", "AT", "GB")
    )

    val changeCountryCodes = Json.obj(
      "countryCodes" -> Json.arr("US", "CH", "FR")
    )

    val postColumn1 = Json.obj("columns" -> Json.arr(columnWithInvalidLanguageType))

    for {
      table <- sendRequest("POST", "/tables", postSimpleTable)
      tableId = table.getLong("id")

      createColumnsJson <- sendRequest("POST", s"/tables/$tableId/columns", postColumn1)
      columnId = createColumnsJson.getJsonArray("columns").getJsonObject(0).getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/columns/1", changeCountryCodes)
      changedColumnsJson <- sendRequest("GET", s"/tables/$tableId/columns")
      changedColumn = changedColumnsJson.getJsonArray("columns").getJsonObject(0)
    } yield {
      assertEquals(Json.arr("US", "CH", "FR"), changedColumn.getJsonArray("countryCodes"))
    }
  }

  @Test
  def changeCountryCodesToEmpty(implicit c: TestContext): Unit = okTest {
    val postSimpleTable = Json.obj("name" -> "table1")
    val columnWithInvalidLanguageType = Json.obj(
      "name" -> "column1",
      "kind" -> "shorttext",
      "languageType" -> "country",
      "countryCodes" -> Json.arr("DE", "AT", "GB")
    )

    val changeCountryCodes = Json.obj(
      "countryCodes" -> Json.arr()
    )

    val postColumn1 = Json.obj("columns" -> Json.arr(columnWithInvalidLanguageType))

    for {
      table <- sendRequest("POST", "/tables", postSimpleTable)
      tableId = table.getLong("id")

      createColumnsJson <- sendRequest("POST", s"/tables/$tableId/columns", postColumn1)
      columnId = createColumnsJson.getJsonArray("columns").getJsonObject(0).getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/columns/1", changeCountryCodes)
      changedColumnsJson <- sendRequest("GET", s"/tables/$tableId/columns")
      changedColumn = changedColumnsJson.getJsonArray("columns").getJsonObject(0)
    } yield {
      assertEquals(Json.arr(), changedColumn.getJsonArray("countryCodes"))
    }
  }

  @Test
  def changeCountryCodesToInvalid(implicit c: TestContext): Unit = exceptionTest("error.json.invalid") {
    val postSimpleTable = Json.obj("name" -> "table1")
    val columnWithInvalidLanguageType = Json.obj(
      "name" -> "column1",
      "kind" -> "shorttext",
      "languageType" -> "country",
      "countryCodes" -> Json.arr("DE", "AT", "GB")
    )

    val changeCountryCodes = Json.obj(
      "countryCodes" -> Json.arr("INVALID")
    )

    val postColumn1 = Json.obj("columns" -> Json.arr(columnWithInvalidLanguageType))

    for {
      table <- sendRequest("POST", "/tables", postSimpleTable)
      tableId = table.getLong("id")

      createColumnsJson <- sendRequest("POST", s"/tables/$tableId/columns", postColumn1)
      columnId = createColumnsJson.getJsonArray("columns").getJsonObject(0).getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/columns/1", changeCountryCodes)
      changedColumnsJson <- sendRequest("GET", s"/tables/$tableId/columns")
      changedColumn = changedColumnsJson.getJsonArray("columns").getJsonObject(0)
    } yield {
      assertEquals(Json.arr(), changedColumn.getJsonArray("countryCodes"))
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

  @Test
  def createMultiCountryColumnWithoutCountryCodes(implicit c: TestContext): Unit = {
    exceptionTest("error.json.countrycodes") {
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
  }
}
