package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.RequestCreation.{Columns, Identifier, Multilanguage, NumericCol, TextCol}
import com.campudus.tableaux.testtools.TableauxTestBase
import com.campudus.tableaux.testtools.TestCustomException

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class CreateCompleteTableTest extends TableauxTestBase {

  @Test
  def createCompleteTable(implicit c: TestContext): Unit = {
    okTest {
      val createCompleteTableJson = Json.obj(
        "name" -> "Test Nr. 1",
        "columns" -> Json.arr(
          Json.obj("kind" -> "text", "name" -> "Test Column 1"),
          Json.obj("kind" -> "numeric", "name" -> "Test Column 2")
        ),
        "rows" -> Json.arr(
          Json.obj("values" -> Json.arr("Test Field 1", 1)),
          Json.obj("values" -> Json.arr("Test Field 2", 2))
        )
      )

      val expectedJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 1,
            "ordering" -> 1,
            "kind" -> "text",
            "name" -> "Test Column 1",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj()
          ),
          Json.obj(
            "id" -> 2,
            "ordering" -> 2,
            "kind" -> "numeric",
            "name" -> "Test Column 2",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj()
          )
        ),
        "rows" -> Json.arr(
          Json.obj("id" -> 1, "values" -> Json.arr("Test Field 1", 1)),
          Json.obj("id" -> 2, "values" -> Json.arr("Test Field 2", 2))
        )
      )

      for {
        test <- sendRequest("POST", "/completetable", createCompleteTableJson)
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }

  @Test
  def createCompleteTableWithOrdering(implicit c: TestContext): Unit = {
    okTest {
      val createCompleteTableJson = Json.obj(
        "name" -> "Test Nr. 1",
        "columns" -> Json.arr(
          Json.obj("kind" -> "text", "name" -> "Test Column 1", "ordering" -> 2),
          Json.obj("kind" -> "numeric", "name" -> "Test Column 2", "ordering" -> 1)
        ),
        "rows" -> Json.arr(
          Json.obj("values" -> Json.arr("Test Field 1", 1)),
          Json.obj("values" -> Json.arr("Test Field 2", 2))
        )
      )

      val expectedJson = Json.obj(
        "id" -> 1,
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 2,
            "ordering" -> 1,
            "kind" -> "numeric",
            "name" -> "Test Column 2",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj()
          ),
          Json.obj(
            "id" -> 1,
            "ordering" -> 2,
            "kind" -> "text",
            "name" -> "Test Column 1",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj()
          )
        ),
        "rows" -> Json.arr(
          Json.obj("id" -> 1, "values" -> Json.arr(1, "Test Field 1")),
          Json.obj("id" -> 2, "values" -> Json.arr(2, "Test Field 2"))
        )
      )

      for {
        test <- sendRequest("POST", "/completetable", createCompleteTableJson)
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }

  @Test
  def createCompleteTableWithoutRows(implicit c: TestContext): Unit = {
    okTest {
      val createCompleteTableJson =
        Json.obj(
          "name" -> "Test Nr. 1",
          "columns" -> Json.arr(
            Json.obj("kind" -> "text", "name" -> "Test Column 1"),
            Json.obj("kind" -> "numeric", "name" -> "Test Column 2")
          )
        )

      val expectedJson = Json.obj(
        "id" -> 1,
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 1,
            "ordering" -> 1,
            "kind" -> "text",
            "name" -> "Test Column 1",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj()
          ),
          Json.obj(
            "id" -> 2,
            "ordering" -> 2,
            "kind" -> "numeric",
            "name" -> "Test Column 2",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj()
          )
        ),
        "rows" -> Json.arr()
      )

      for {
        test <- sendRequest("POST", "/completetable", createCompleteTableJson)
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class CreateRowTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")

  def createTextColumnJson(name: String) = Columns().add(TextCol(name)).getJson
  def createNumberColumnJson(name: String) = Columns().add(NumericCol(name)).getJson
  def createMultilanguageTextColumnJson(name: String) = Columns().add(Multilanguage(TextCol(name))).getJson

  @Test
  def createRow_ok(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 1, "values" -> Json.arr())
    val expectedJson2 = Json.obj("status" -> "ok", "id" -> 2, "values" -> Json.arr())

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/rows")
      test2 <- sendRequest("POST", "/tables/1/rows")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createRowWithValues_withSinglelanguageValueForMultilanguageColumn_throwsException(implicit c: TestContext): Unit =
    okTest {
      val valuesRow = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1")))
      )
      val expectedException = TestCustomException(
        "Invalid value (JSON required) for MultiLanguage column Test Column 1, Multilanguage",
        "error.arguments",
        422
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", createMultilanguageTextColumnJson("Test Column 1, Multilanguage"))

        test <- sendRequest("POST", "/tables/1/rows", valuesRow).toException()
      } yield {
        assertEquals(expectedException, test)
      }
    }

  @Test
  def createRowWithValues_withMultilanguageValueForSinglelanguageColumn_throwsException(implicit c: TestContext): Unit =
    okTest {
      val valuesRow = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr(Json.obj("de-DE" -> "Test Field 1"))))
      )
      val expectedException = TestCustomException(
        "Invalid value (single value required) for SingleLanguage column Test Column 1",
        "error.arguments",
        422
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", createTextColumnJson("Test Column 1"))

        test <- sendRequest("POST", "/tables/1/rows", valuesRow).toException()
      } yield {
        assertEquals(expectedException, test)
      }
    }

  @Test
  def createRowWithValues_withTextValueForNumericColumn_throwsException(implicit c: TestContext): Unit =
    okTest {
      val valuesRow = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr("A text instead of a number")))
      )
      val expectedException = TestCustomException(
        "Invalid value for numeric column Number Test Column 1, expected a number",
        "error.arguments",
        422
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson("Number Test Column 1"))

        test <- sendRequest("POST", "/tables/1/rows", valuesRow).toException()
      } yield {
        assertEquals(expectedException, test)
      }
    }

  @Test
  def createFullRow(implicit c: TestContext): Unit = {
    okTest {
      val valuesRow = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)))
      )
      val expectedJson =
        Json.obj("status" -> "ok", "rows" -> Json.arr(Json.obj("id" -> 1, "values" -> Json.arr("Test Field 1", 2))))

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", createTextColumnJson("Test Column 1"))
        _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson("Test Column 2"))
        test <- sendRequest("POST", "/tables/1/rows", valuesRow)
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }

  @Test
  def createComplexRow(implicit c: TestContext): Unit = {
    okTest {
      val fileName = "Scr$en Shot.pdf"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val mimeType = "application/pdf"
      val de = "de-DE"
      val en = "en-GB"

      val putOne = Json.obj(
        "title" -> Json.obj(de -> "Ein schöner deutscher Titel."),
        "description" -> Json.obj(de -> "Und hier folgt eine tolle hochdeutsche Beschreibung.")
      )

      def valuesRow(linkToRowId: Long, fileUuid: String) = {
        Json.obj(
          "columns" -> Json.arr(
            Json.obj("id" -> 1), // text
            Json.obj("id" -> 2), // text multilanguage
            Json.obj("id" -> 3), // numeric
            Json.obj("id" -> 4), // numeric multilanguage
            Json.obj("id" -> 5), // richtext
            Json.obj("id" -> 6), // richtext multilanguage
            Json.obj("id" -> 7), // date
            Json.obj("id" -> 8), // date multilanguage
            Json.obj("id" -> 9), // attachment
            Json.obj("id" -> 10) // link
          ),
          "rows" -> Json.arr(
            Json.obj(
              "values" ->
                Json.arr(
                  "Test Field in first row, column one",
                  Json.obj(de -> "Erste Zeile, Spalte 2", en -> "First row, column 2"),
                  3,
                  Json.obj(de -> 14),
                  "Test Field in first row, column <strong>five</strong>.",
                  Json.obj(
                    "de_AT" -> "Erste Reihe, Spalte <strong>sechs</strong> - mit AT statt DE.",
                    "en_GB" -> "First row, column <strong>six</strong> - with GB instead of US."
                  ),
                  "2016-01-08",
                  Json.obj(en -> "2016-01-08"),
                  Json.obj("uuid" -> fileUuid),
                  Json.obj("to" -> linkToRowId)
                )
            )
          )
        )
      }

      def expectedWithoutAttachment(rowId: Long, fileUuid: String, linkToRowId: Long) = Json.obj(
        "status" -> "ok",
        "id" -> rowId,
        "values" ->
          Json.arr(
            "Test Field in first row, column one",
            Json.obj(de -> "Erste Zeile, Spalte 2", en -> "First row, column 2"),
            3,
            Json.obj(de -> 14),
            "Test Field in first row, column <strong>five</strong>.",
            Json.obj(
              "de_AT" -> "Erste Reihe, Spalte <strong>sechs</strong> - mit AT statt DE.",
              "en_GB" -> "First row, column <strong>six</strong> - with GB instead of US."
            ),
            "2016-01-08",
            Json.obj(en -> "2016-01-08"),
            Json.arr(
              Json.obj(
                "id" -> linkToRowId,
                "value" -> Json.obj(
                  de -> "Hallo, Test Table 1 Welt!",
                  en -> "Hello, Test Table 1 World!"
                )
              )
            )
          )
      )

      for {
        (tableId1, columnIds, rowIds) <- createFullTableWithMultilanguageColumns("Test Table 1")
        (tableId2, columnIds, linkColumnId) <- createTableWithComplexColumns("Test Table 2", tableId1)

        file <- sendRequest("POST", "/files", putOne)
        fileUuid = file.getString("uuid")
        uploadedFile <- uploadFile("PUT", s"/files/$fileUuid/$de", filePath, mimeType)

        // link to table 1, row 1, column 1
        row <- sendRequest("POST", s"/tables/$tableId2/rows", valuesRow(rowIds.head, fileUuid))
        rowId = row.getJsonArray("rows").getJsonObject(0).getLong("id")

        result <- sendRequest("GET", s"/tables/$tableId2/rows/$rowId")
      } yield {
        val expect = expectedWithoutAttachment(rowId, fileUuid, rowIds.head)

        val resultAttachment = result.getJsonArray("values").getJsonArray(8)
        logger.info(s"expect=${expect.encode()}")
        logger.info(s"result=${result.encode()}")
        assertEquals(1, resultAttachment.size)
        assertEquals(fileUuid, resultAttachment.getJsonObject(0).getString("uuid"))

        result.getJsonArray("values").remove(8)
        assertJSONEquals(expect, result)
      }
    }
  }

  @Test
  def createMultipleFullRows(implicit c: TestContext): Unit = {
    okTest {
      val valuesRow = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
        "rows" -> Json.arr(
          Json.obj("values" -> Json.arr("Test Field 1", 2)),
          Json.obj("values" -> Json.arr("Test Field 2", 5))
        )
      )
      val expectedJson = Json.obj(
        "status" -> "ok",
        "rows" -> Json.arr(
          Json.obj("id" -> 1, "values" -> Json.arr("Test Field 1", 2)),
          Json.obj("id" -> 2, "values" -> Json.arr("Test Field 2", 5))
        )
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", createTextColumnJson("Test Column 1"))
        _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson("Test Column 2"))
        test <- sendRequest("POST", "/tables/1/rows", valuesRow)
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }

  @Test
  def createEmptyRows(implicit c: TestContext): Unit = {
    okTest {
      val createCompleteTableJson =
        Json.obj(
          "name" -> "Test Nr. 1",
          "columns" -> Json.arr(
            Json.obj("kind" -> "text", "name" -> "Test Column 1"),
            Json.obj("kind" -> "numeric", "name" -> "Test Column 2")
          )
        )

      for {
        _ <- sendRequest("POST", "/completetable", createCompleteTableJson)

        row1 <- sendRequest("POST", "/tables/1/rows", "null") map (_.getInteger("id"))
        row2 <- sendRequest("POST", "/tables/1/rows", "") map (_.getInteger("id"))
        row3 <- sendRequest("POST", "/tables/1/rows", "{}") map (_.getInteger("id"))
      } yield {
        assertEquals(1, row1)
        assertEquals(2, row2)
        assertEquals(3, row3)
      }
    }
  }

  @Test
  def createEmptyRowAndCheckValues(implicit c: TestContext): Unit = okTest {
    for {
      (tableId1, columnId, rowId) <- createSimpleTableWithCell("table1", Identifier(TextCol("name")))
      (tableId2, columnIds, linkColumnId) <- createTableWithComplexColumns("Test Table 2", tableId1)
      row <- sendRequest("POST", s"/tables/$tableId2/rows")
      rowId = row.getLong("id")
      cells <- Future.sequence((columnIds :+ linkColumnId).map(columnId => {
        sendRequest("GET", s"/tables/$tableId2/columns/$columnId/rows/$rowId").map(_.getValue("value"))
      }))
    } yield {
      assertEquals(Json.arr(cells: _*), row.getJsonArray("values"))
    }
  }
}
