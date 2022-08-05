package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class DuplicateRowTest extends TableauxTestBase {

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
  def duplicateRowWithLink(implicit c: TestContext): Unit = {
    okTest {
      val postLinkCol =
        Json.obj("columns" -> Json.arr(Json.obj("name" -> "Test Link 1", "kind" -> "link", "toTable" -> 2)))

      def fillLinkCellJson(c: Integer) = Json.obj("value" -> Json.obj("to" -> c))

      for {
        tableId1 <- createDefaultTable()
        tableId2 <- createDefaultTable("Test Table 2", 2)
        linkColumn <- sendRequest("POST", s"/tables/$tableId1/columns", postLinkCol)
        linkColumnId = linkColumn.getJsonArray("columns").getJsonObject(0).getNumber("id")
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
  }

  @Test
  def duplicateRowWithLinkWithConcatColumn(implicit c: TestContext): Unit = {
    okTest {
      val postLinkCol =
        Json.obj("columns" -> Json.arr(Json.obj("name" -> "Test Link 1", "kind" -> "link", "toTable" -> 2)))

      def fillLinkCellJson(c: Integer) = Json.obj("value" -> Json.obj("to" -> c))
      val postIdentifier = Json.obj("identifier" -> true)

      for {
        tableId1 <- createDefaultTable()
        tableId2 <- createDefaultTable("Test Table 2", 2)
        _ <- sendRequest("POST", s"/tables/$tableId2/columns/1", postIdentifier)
        _ <- sendRequest("POST", s"/tables/$tableId2/columns/2", postIdentifier)
        linkColumn <- sendRequest("POST", s"/tables/$tableId1/columns", postLinkCol)
        linkColumnId = linkColumn.getJsonArray("columns").getJsonObject(0).getNumber("id")
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
  }

  @Test
  def duplicateRowWithMultiLanguageAttachment(implicit c: TestContext): Unit = {
    okTest {
      val postAttachmentColumn = Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "kind" -> "attachment",
            "name" -> "Downloads"
          )
        )
      )
      val fileName = "Scr$en Shot.pdf"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val mimeType = "application/pdf"
      val de = "de-DE"

      val putOne = Json.obj(
        "title" -> Json.obj(de -> "Ein schöner deutscher Titel."),
        "description" -> Json.obj(de -> "Und hier folgt eine tolle hochdeutsche Beschreibung.")
      )

      def insertRow(uuid: String) = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2), Json.obj("id" -> 3)),
        "rows" -> Json.arr(
          Json.obj(
            "values" -> Json.arr(
              "row 3 column 1",
              3,
              Json.obj("uuid" -> uuid)
            )
          )
        )
      )

      for {
        tableId <- createDefaultTable()
        column <- sendRequest("POST", s"/tables/$tableId/columns", postAttachmentColumn)
        columnId = column.getJsonArray("columns").getJsonObject(0).getInteger("id")
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
        assertJSONEquals(expected, result)
      }
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

}
