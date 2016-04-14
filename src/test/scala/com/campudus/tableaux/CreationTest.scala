package com.campudus.tableaux

import java.util

import com.campudus.tableaux.testtools.RequestCreation
import com.campudus.tableaux.testtools.RequestCreation.{Identifier, Text}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
class CreationTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")

  def createTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.Text(name)).getJson

  def createShortTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.ShortText(name)).getJson

  def createRichTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.RichText(name)).getJson

  def createNumberColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.Numeric(name)).getJson

  def createBooleanColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.Boolean(name)).getJson

  @Test
  def createTable(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 1, "hidden" -> false).mergeIn(createTableJson)
    val expectedJson2 = Json.obj("status" -> "ok", "id" -> 2, "hidden" -> false).mergeIn(createTableJson)

    for {
      test1 <- sendRequest("POST", "/tables", createTableJson)
      test2 <- sendRequest("POST", "/tables", createTableJson)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createTextColumn(implicit c: TestContext): Unit = okTest {
    val createColumn1 = createTextColumnJson("column1")
    val createColumn2 = createTextColumnJson("column2")

    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
      test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createShortTextColumn(implicit c: TestContext): Unit = okTest {
    val createColumn1 = createShortTextColumnJson("column1")
    val createColumn2 = createShortTextColumnJson("column2")

    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
      test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createRichTextColumn(implicit c: TestContext): Unit = okTest {
    val createColumn1 = createRichTextColumnJson("column1")
    val createColumn2 = createRichTextColumnJson("column2")

    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
      test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createNumberColumn(implicit c: TestContext): Unit = okTest {
    val createColumn1 = createNumberColumnJson("column1")
    val createColumn2 = createNumberColumnJson("column2")
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
      test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createBooleanColumn(implicit c: TestContext): Unit = okTest {
    val createColumn1 = createBooleanColumnJson("column1")
    val createColumn2 = createBooleanColumnJson("column2")

    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn1.getJsonArray("columns").getJsonObject(0))))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()).mergeIn(createColumn2.getJsonArray("columns").getJsonObject(0))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createColumn1)
      test2 <- sendRequest("POST", "/tables/1/columns", createColumn2)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createMultipleColumns(implicit c: TestContext): Unit = okTest {
    val jsonObj = Json.obj("columns" -> Json.arr(
      Json.obj("kind" -> "numeric", "name" -> "Test Column 1"),
      Json.obj("kind" -> "text", "name" -> "Test Column 2")))

    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(
      Json.obj("id" -> 1, "ordering" -> 1, "kind" -> "numeric", "name" -> "Test Column 1", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()),
      Json.obj("id" -> 2, "ordering" -> 2, "kind" -> "text", "name" -> "Test Column 2", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj())))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test <- sendRequest("POST", "/tables/1/columns", jsonObj)
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createMultipleColumnsWithOrdering(implicit c: TestContext): Unit = okTest {
    val jsonObj = Json.obj("columns" -> Json.arr(
      Json.obj("kind" -> "numeric", "name" -> "Test Column 1", "ordering" -> 2),
      Json.obj("kind" -> "text", "name" -> "Test Column 2", "ordering" -> 1)))
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(
      Json.obj("id" -> 2, "ordering" -> 1, "kind" -> "text", "name" -> "Test Column 2", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()),
      Json.obj("id" -> 1, "ordering" -> 2, "kind" -> "numeric", "name" -> "Test Column 1", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj())))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test <- sendRequest("POST", "/tables/1/columns", jsonObj)
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createRow(implicit c: TestContext): Unit = okTest {
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
  def createFullRow(implicit c: TestContext): Unit = okTest {
    val valuesRow = Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2))))
    val expectedJson = Json.obj("status" -> "ok", "rows" -> Json.arr(Json.obj("id" -> 1, "values" -> Json.arr("Test Field 1", 2))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createTextColumnJson("Test Column 1"))
      _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson("Test Column 2"))
      test <- sendRequest("POST", "/tables/1/rows", valuesRow)
    } yield {
      assertContains(expectedJson, test)
    }
  }

  @Test
  def createComplexRow(implicit c: TestContext): Unit = okTest {
    val fileName = "Scr$en Shot.pdf"
    val filePath = s"/com/campudus/tableaux/uploads/$fileName"
    val mimeType = "application/pdf"
    val de = "de_DE"
    val en = "en_US"

    val putOne = Json.obj(
      "title" -> Json.obj(de -> "Ein schöner deutscher Titel."),
      "description" -> Json.obj(de -> "Und hier folgt eine tolle hochdeutsche Beschreibung.")
    )

    def valuesRow(linkToRowId: Long, fileUuid: String) = Json.obj(
      "columns" -> Json.arr(
        Json.obj("id" -> 1), //text
        Json.obj("id" -> 2), //text multilanguage
        Json.obj("id" -> 3), //numeric
        Json.obj("id" -> 4), //numeric multilanguage
        Json.obj("id" -> 5), //richtext
        Json.obj("id" -> 6), //richtext multilanguage
        Json.obj("id" -> 7), //date
        Json.obj("id" -> 8), //date multilanguage
        Json.obj("id" -> 9), //attachment
        Json.obj("id" -> 10) // link
      ),
      "rows" -> Json.arr(Json.obj("values" ->
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
        ))))

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
          Json.arr(Json.obj("id" -> linkToRowId, "value" -> Json.obj(
            de -> "Hallo, Test Table 1 Welt!",
            en -> "Hello, Test Table 1 World!"
          )))
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
      val resultAttachment = result.getJsonArray("values").remove(8).asInstanceOf[util.ArrayList[_]]
      logger.info(s"expect=${expect.encode()}")
      logger.info(s"result=${result.encode()}")
      assertEquals(1, resultAttachment.size)
      assertEquals(fileUuid, resultAttachment.get(0).asInstanceOf[util.Map[String, _]].get("uuid"))
      assertEquals(expect, result)
    }
  }

  @Test
  def duplicateRowWithConcatColumn(implicit c: TestContext): Unit = okTest {
    val postIdentifier = Json.obj("identifier" -> true)

    for {
      table <- createDefaultTable()

      _ <- sendRequest("POST", s"/tables/$table/columns/1", postIdentifier)
      _ <- sendRequest("POST", s"/tables/$table/columns/2", postIdentifier)

      expected <- sendRequest("GET", "/tables/1/rows/1")

      duplicated <- sendRequest("POST", "/tables/1/rows/1/duplicate")
      result <- sendRequest("GET", s"/tables/1/rows/${duplicated.getNumber("id")}")
    } yield {
      assertEquals(result, duplicated)

      assertNotSame(expected.getNumber("id"), result.getNumber("id"))

      expected.remove("id")
      result.remove("id")

      assertEquals(expected, result)
    }
  }

  @Test
  def duplicateRowWithLink(implicit c: TestContext): Unit = okTest {
    val postLinkCol = Json.obj("columns" -> Json.arr(Json.obj("name" -> "Test Link 1", "kind" -> "link", "toTable" -> 2)))
    def fillLinkCellJson(c: Integer) = Json.obj("value" -> Json.obj("to" -> c))

    for {
      tableId1 <- createDefaultTable()
      tableId2 <- createDefaultTable("Test Table 2", 2)
      linkColumn <- sendRequest("POST", s"/tables/$tableId1/columns", postLinkCol)
      linkColumnId = linkColumn.getArray("columns").getJsonObject(0).getNumber("id")
      _ <- sendRequest("POST", s"/tables/$tableId1/columns/$linkColumnId/rows/1", fillLinkCellJson(1))
      _ <- sendRequest("POST", s"/tables/$tableId1/columns/$linkColumnId/rows/1", fillLinkCellJson(2))
      expected <- sendRequest("GET", "/tables/1/rows/1")
      duplicatedPost <- sendRequest("POST", "/tables/1/rows/1/duplicate")
      result <- sendRequest("GET", s"/tables/1/rows/${duplicatedPost.getNumber("id")}")
    } yield {
      assertEquals(result, duplicatedPost)

      assertNotSame(expected.getNumber("id"), result.getNumber("id"))
      expected.remove("id")
      result.remove("id")
      logger.info(s"expected without id=${expected.encode()}")
      logger.info(s"result without id=${result.encode()}")
      assertEquals(expected, result)
    }
  }

  @Test
  def duplicateRowWithLinkWithConcatColumn(implicit c: TestContext): Unit = okTest {
    val postLinkCol = Json.obj("columns" -> Json.arr(Json.obj("name" -> "Test Link 1", "kind" -> "link", "toTable" -> 2)))
    def fillLinkCellJson(c: Integer) = Json.obj("value" -> Json.obj("to" -> c))
    val postIdentifier = Json.obj("identifier" -> true)

    for {
      tableId1 <- createDefaultTable()
      tableId2 <- createDefaultTable("Test Table 2", 2)
      _ <- sendRequest("POST", s"/tables/$tableId2/columns/1", postIdentifier)
      _ <- sendRequest("POST", s"/tables/$tableId2/columns/2", postIdentifier)
      linkColumn <- sendRequest("POST", s"/tables/$tableId1/columns", postLinkCol)
      linkColumnId = linkColumn.getArray("columns").getJsonObject(0).getNumber("id")
      _ <- sendRequest("POST", s"/tables/$tableId1/columns/$linkColumnId/rows/1", fillLinkCellJson(1))
      _ <- sendRequest("POST", s"/tables/$tableId1/columns/$linkColumnId/rows/1", fillLinkCellJson(2))
      expected <- sendRequest("GET", s"/tables/$tableId1/rows/1")
      duplicatedPost <- sendRequest("POST", s"/tables/$tableId1/rows/1/duplicate")
      result <- sendRequest("GET", s"/tables/1/rows/${duplicatedPost.getNumber("id")}")
    } yield {
      assertEquals(result, duplicatedPost)

      assertNotSame(expected.getNumber("id"), result.getNumber("id"))
      expected.remove("id")
      result.remove("id")
      logger.info(s"expected without id=${expected.encode()}")
      logger.info(s"result without id=${result.encode()}")
      assertEquals(expected, result)
    }
  }

  @Test
  def duplicateRowWithMultiLanguageAttachment(implicit c: TestContext): Unit = okTest {
    val postAttachmentColumn = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "attachment",
      "name" -> "Downloads"
    )))
    val fileName = "Scr$en Shot.pdf"
    val filePath = s"/com/campudus/tableaux/uploads/$fileName"
    val mimeType = "application/pdf"
    val de = "de_DE"

    val putOne = Json.obj(
      "title" -> Json.obj(de -> "Ein schöner deutscher Titel."),
      "description" -> Json.obj(de -> "Und hier folgt eine tolle hochdeutsche Beschreibung.")
    )

    def insertRow(uuid: String) = Json.obj(
      "columns" -> Json.arr(
        Json.obj("id" -> 1),
        Json.obj("id" -> 2),
        Json.obj("id" -> 3)),
      "rows" -> Json.arr(Json.obj("values" -> Json.arr(
        "row 3 column 1",
        3,
        Json.obj("uuid" -> uuid)
      )))
    )

    for {
      tableId <- createDefaultTable()
      column <- sendRequest("POST", s"/tables/$tableId/columns", postAttachmentColumn)
      columnId = column.getArray("columns").getJsonObject(0).getInteger("id")
      file <- sendRequest("POST", "/files", putOne)
      uploadedFile <- uploadFile("PUT", s"/files/${file.getString("uuid")}/$de", filePath, mimeType)
      row <- sendRequest("POST", s"/tables/$tableId/rows", insertRow(file.getString("uuid")))
      rowId = row.getJsonArray("rows").getJsonObject(0).getInteger("id")
      expected <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      duplicatedPost <- sendRequest("POST", s"/tables/$tableId/rows/$rowId/duplicate")
      result <- sendRequest("GET", s"/tables/$tableId/rows/${duplicatedPost.getNumber("id")}")
    } yield {
      logger.info(s"expected=${expected.encode()}")
      logger.info(s"result=${result.encode()}")
      assertEquals(result, duplicatedPost)
      assertNotSame(expected.getNumber("id"), result.getNumber("id"))
      expected.remove("id")
      result.remove("id")
      assertContains(expected, result)
    }
  }

  @Test
  def duplicateEmptyRow(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createDefaultTable()

      emptyRow <- sendRequest("POST", s"/tables/$tableId/rows")
      emptyRowId = emptyRow.getLong("id")

      duplicatedRow <- sendRequest("POST", s"/tables/$tableId/rows/$emptyRowId/duplicate")
      duplicatedRowId = duplicatedRow.getLong("id")

      expected <- sendRequest("GET", s"/tables/$tableId/rows/$emptyRowId")
      actual <- sendRequest("GET", s"/tables/$tableId/rows/$duplicatedRowId")
    } yield {
      logger.info(s"expected=${expected.encode()}")
      logger.info(s"actual=${actual.encode()}")
      assertNotSame(expected, actual)
      expected.remove("id")
      actual.remove("id")
      assertEquals(expected, actual)
    }
  }

  @Test
  def duplicateEmptyRowWithMultilanguageValues(implicit c: TestContext): Unit = okTest {
    for {
      (tableId, _) <- createTableWithMultilanguageColumns("some_table")

      emptyRow <- sendRequest("POST", s"/tables/$tableId/rows")
      emptyRowId = emptyRow.getLong("id")

      duplicatedRow <- sendRequest("POST", s"/tables/$tableId/rows/$emptyRowId/duplicate")
      duplicatedRowId = duplicatedRow.getLong("id")

      expected <- sendRequest("GET", s"/tables/$tableId/rows/$emptyRowId")
      actual <- sendRequest("GET", s"/tables/$tableId/rows/$duplicatedRowId")
    } yield {
      logger.info(s"expected=${expected.encode()}")
      logger.info(s"actual=${actual.encode()}")
      assertNotSame(expected, actual)
      expected.remove("id")
      actual.remove("id")
      assertEquals(expected, actual)
    }
  }

  @Test
  def createMultipleFullRows(implicit c: TestContext): Unit = okTest {
    val valuesRow = Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
      "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)), Json.obj("values" -> Json.arr("Test Field 2", 5))))
    val expectedJson = Json.obj("status" -> "ok", "rows" -> Json.arr(
      Json.obj("id" -> 1, "values" -> Json.arr("Test Field 1", 2)), Json.obj("id" -> 2, "values" -> Json.arr("Test Field 2", 5))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createTextColumnJson("Test Column 1"))
      _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson("Test Column 2"))
      test <- sendRequest("POST", "/tables/1/rows", valuesRow)
    } yield {
      assertContains(expectedJson, test)
    }
  }

  @Test
  def createCompleteTable(implicit c: TestContext): Unit = okTest {
    val createCompleteTableJson = Json.obj(
      "name" -> "Test Nr. 1",
      "columns" -> Json.arr(
        Json.obj("kind" -> "text", "name" -> "Test Column 1"),
        Json.obj("kind" -> "numeric", "name" -> "Test Column 2")),
      "rows" -> Json.arr(
        Json.obj("values" -> Json.arr("Test Field 1", 1)),
        Json.obj("values" -> Json.arr("Test Field 2", 2))))

    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "ordering" -> 1, "kind" -> "text", "name" -> "Test Column 1", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()),
        Json.obj("id" -> 2, "ordering" -> 2, "kind" -> "numeric", "name" -> "Test Column 2", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj())),
      "rows" -> Json.arr(
        Json.obj("id" -> 1, "values" -> Json.arr("Test Field 1", 1)),
        Json.obj("id" -> 2, "values" -> Json.arr("Test Field 2", 2))))

    for {
      test <- sendRequest("POST", "/completetable", createCompleteTableJson)
    } yield {
      assertContains(expectedJson, test)
    }
  }

  @Test
  def createCompleteTableWithOrdering(implicit c: TestContext): Unit = okTest {
    val createCompleteTableJson = Json.obj(
      "name" -> "Test Nr. 1",
      "columns" -> Json.arr(
        Json.obj("kind" -> "text", "name" -> "Test Column 1", "ordering" -> 2),
        Json.obj("kind" -> "numeric", "name" -> "Test Column 2", "ordering" -> 1)),
      "rows" -> Json.arr(
        Json.obj("values" -> Json.arr("Test Field 1", 1)),
        Json.obj("values" -> Json.arr("Test Field 2", 2))))

    val expectedJson = Json.obj(
      "id" -> 1,
      "columns" -> Json.arr(
        Json.obj("id" -> 2, "ordering" -> 1, "kind" -> "numeric", "name" -> "Test Column 2", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()),
        Json.obj("id" -> 1, "ordering" -> 2, "kind" -> "text", "name" -> "Test Column 1", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj())
      ),
      "rows" -> Json.arr(
        Json.obj("id" -> 1, "values" -> Json.arr(1, "Test Field 1")),
        Json.obj("id" -> 2, "values" -> Json.arr(2, "Test Field 2"))))

    for {
      test <- sendRequest("POST", "/completetable", createCompleteTableJson)
    } yield {
      assertContains(expectedJson, test)
    }
  }

  @Test
  def createCompleteTableWithoutRows(implicit c: TestContext): Unit = okTest {
    val createCompleteTableJson = Json.obj(
      "name" -> "Test Nr. 1",
      "columns" -> Json.arr(
        Json.obj("kind" -> "text", "name" -> "Test Column 1"),
        Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))

    val expectedJson = Json.obj(
      "id" -> 1,
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "ordering" -> 1, "kind" -> "text", "name" -> "Test Column 1", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()),
        Json.obj("id" -> 2, "ordering" -> 2, "kind" -> "numeric", "name" -> "Test Column 2", "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj())),
      "rows" -> Json.arr())

    for {
      test <- sendRequest("POST", "/completetable", createCompleteTableJson)
    } yield {
      assertContains(expectedJson, test)
    }
  }

  @Test
  def createEmptyRows(implicit c: TestContext): Unit = okTest {
    val createCompleteTableJson = Json.obj(
      "name" -> "Test Nr. 1",
      "columns" -> Json.arr(
        Json.obj("kind" -> "text", "name" -> "Test Column 1"),
        Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))

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

  @Test
  def createEmptyRowAndCheckValues(implicit c: TestContext): Unit = okTest {
    for {
      (tableId1, columnId, rowId) <- createSimpleTableWithCell("table1", Identifier(Text("name")))
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