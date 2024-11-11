package com.campudus.tableaux.api.content

import com.campudus.tableaux.database.domain.Cardinality
import com.campudus.tableaux.database.domain.CellAnnotationType
import com.campudus.tableaux.database.domain.Constraint
import com.campudus.tableaux.database.model.TableauxModel
import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.testtools.JsonAssertable
import com.campudus.tableaux.testtools.RequestCreation
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

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

  @Test
  def skipConstrainedLinks(implicit c: TestContext): Unit = okTest {
    for {
      tableIds <- createLinkedTables()
      modelTableId = tableIds(0)
      _ <- sendRequest("POST", s"/tables/$modelTableId/rows/1/duplicate?skipConstrainedLinks=true")
      rowRes <- sendRequest("GET", s"/tables/$modelTableId/rows")
      annotationRes <- sendRequest("GET", s"/tables/$modelTableId/columns/3/rows/3/annotations")
    } yield {
      val rows = rowRes.getJsonArray("rows")
      val originalValues = rows.getJsonObject(0).getJsonArray("values")
      val duplicatedValues = rows.getJsonObject(2).getJsonArray("values")

      assertEquals(originalValues.getString(0), duplicatedValues.getString(0))
      assertEquals(1, originalValues.getJsonArray(2).size()) // original had a link
      assertEquals(0, duplicatedValues.getJsonArray(2).size()) // duplicate does not have it
      assertEquals(null, annotationRes.getJsonArray("annotations").getJsonObject(0)) // no annotations set
    }
  }

  @Test
  def skipAndAnnotateConstrainedLinks(implicit c: TestContext): Unit = okTest {
    for {
      tableIds <- createLinkedTables()
      modelTableId = tableIds(0)
      _ <- sendRequest("POST", s"/tables/$modelTableId/rows/1/duplicate?skipConstrainedLinks=true&annotateSkipped=true")
      res <- sendRequest("GET", s"/tables/$modelTableId/columns/3/rows/3/annotations")
    } yield {
      val checkMeAnnotation = res.getJsonArray("annotations").getJsonArray(0).getJsonObject(0)
      assertEquals(checkMeAnnotation.getString("type"), CellAnnotationType.FLAG)
      assertEquals(checkMeAnnotation.getString("value"), "check-me")
    }
  }

  @Test
  def duplicateSpecificRows(implicit c: TestContext): Unit = okTest {
    val payload = Json.obj("columns" -> Json.arr(Json.obj("id" -> 1)))
    for {
      tableId <- createDefaultTable(name = "models")
      _ <- sendRequest("POST", s"/tables/$tableId/rows/1/duplicate?skipConstrainedLinks=true", payload)
      rowRes <- sendRequest("GET", s"/tables/$tableId/rows")
    } yield {
      val rows = rowRes.getJsonArray("rows")
      val originalValues = rows.getJsonObject(0).getJsonArray("values")
      val duplicatedValues = rows.getJsonObject(2).getJsonArray("values")

      assertEquals(originalValues.getString(0), duplicatedValues.getString(0))
      assertEquals(null, duplicatedValues.getJsonObject(1)) // column not in payload was omitted
    }
  }

  @Test
  def duplicateSpecificRowsWithoutConstrainedLinks(implicit c: TestContext): Unit = okTest {
    val payload = Json.obj("columns" -> Json.arr(
      Json.obj("id" -> 1),
      Json.obj("id" -> 3)
    ))
    for {
      tableIds <- createLinkedTables()
      modelTableId = tableIds(0)
      _ <- sendRequest("POST", s"/tables/$modelTableId/rows/1/duplicate?skipConstrainedLinks=true", payload)
      rowRes <- sendRequest("GET", s"/tables/$modelTableId/rows")
    } yield {
      val rows = rowRes.getJsonArray("rows")
      val originalValues = rows.getJsonObject(0).getJsonArray("values")
      val duplicatedValues = rows.getJsonObject(2).getJsonArray("values")

      assertEquals(originalValues.getString(0), duplicatedValues.getString(0))
      assertEquals(null, duplicatedValues.getJsonObject(1)) // column not in payload was omitted
      assertEquals(0, duplicatedValues.getJsonArray(2).size()) // constrained link was still skipped
    }
  }

  def createLinkedTables(): Future[Seq[TableId]] = {
    for {
      modelTableId <- createDefaultTable(name = "models")
      variantTableId <- createDefaultTable(name = "variants")
      addLinkColumn = RequestCreation.Columns(
        RequestCreation.LinkBiDirectionalCol(
          "variant",
          variantTableId,
          Constraint(new Cardinality(1, 0), deleteCascade = false)
        )
      )
      _ <- sendRequest("POST", s"/tables/$modelTableId/columns", addLinkColumn)
      _ <- sendRequest(
        "POST",
        s"/tables/$modelTableId/columns/3/rows/1",
        Json.obj("value" -> Json.arr(1))
      )
    } yield (
      Seq(modelTableId, variantTableId)
    )
  }
}
