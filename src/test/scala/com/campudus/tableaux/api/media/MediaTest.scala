package com.campudus.tableaux.api.media

import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.testtools.{RequestCreation, TableauxTestBase, TestCustomException}

import io.vertx.core.buffer.Buffer
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.FutureHelper._
import io.vertx.scala.core.http.{HttpClient, HttpClientResponse}
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.{Future, Promise}
import scala.reflect.io.Path

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

trait MediaTestBase extends TableauxTestBase {

  protected def createFile(langtag: String, filePath: String, mimeType: String, folder: Option[FolderId])(
      implicit c: TestContext
  ): Future[JsonObject] = {
    val meta = Json.obj(
      "title" -> Json.obj(
        langtag -> filePath
      ),
      "description" -> Json.obj(
        langtag -> filePath
      ),
      "folder" -> folder.orNull
    )

    for {
      created <- sendRequest("POST", "/files", meta)
      uploaded <- replaceFile(created.getString("uuid"), langtag, filePath, mimeType)
    } yield uploaded
  }

  protected def replaceFile(uuid: String, langtag: String, file: String, mimeType: String)(
      implicit c: TestContext
  ): Future[JsonObject] = {
    uploadFile("PUT", s"/files/$uuid/$langtag", file, mimeType)
  }
}

@RunWith(classOf[VertxUnitRunner])
class AttachmentTest extends MediaTestBase {

  @Test
  def testCreateAttachmentColumn(implicit c: TestContext): Unit = {
    okTest {
      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 3,
            "ordering" -> 3,
            "name" -> "Downloads",
            "kind" -> "attachment",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj()
          )
        )
      )

      val column = Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "kind" -> "attachment",
            "name" -> "Downloads"
          )
        )
      )

      for {
        tableId <- createDefaultTable()

        column <- sendRequest("POST", s"/tables/$tableId/columns", column)
      } yield {
        assertJSONEquals(expectedJson, column)
      }
    }
  }

  @Test
  def testFillAndRetrieveAttachmentCell(implicit c: TestContext): Unit = {
    okTest {
      val column = Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "kind" -> "attachment",
            "name" -> "Downloads"
          )
        )
      )

      val fileName = "Scr$en Shot.pdf"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"
      val putFile = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test PDF"),
        "description" -> Json.obj("de-DE" -> "A description about that PDF.")
      )

      for {
        tableId <- createDefaultTable()

        columnId <- sendRequest("POST", s"/tables/$tableId/columns", column)
          .map(_.getJsonArray("columns").get[JsonObject](0).getInteger("id"))

        rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))

        fileUuid <- createFile("de-DE", filePath, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid", putFile)

        // Add attachment
        resultFill <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid))
        )

        // Retrieve attachment
        resultRetrieve <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        _ <- sendRequest("DELETE", s"/files/$fileUuid")
      } yield {
        assertEquals(3, columnId)

        val uuid = resultFill.getJsonArray("value").getJsonObject(0).getString("uuid")
        assertNotNull(uuid)
        assertJSONEquals(
          Json.obj(
            "ordering" -> 1,
            "folder" -> null,
            "uuid" -> uuid,
            "title" -> Json.obj("de-DE" -> "Test PDF"),
            "url" -> Json.obj("de-DE" -> s"/files/$uuid/de-DE/Scr%24en+Shot.pdf"),
            "description" -> Json.obj("de-DE" -> "A description about that PDF."),
            "externalName" -> Json.obj("de-DE" -> "Scr$en Shot.pdf"),
            "mimeType" -> Json.obj("de-DE" -> "application/pdf")
          ),
          resultFill.getJsonArray("value").getJsonObject(0)
        )

        assertEquals(fileUuid, resultRetrieve.getJsonArray("value").get[JsonObject](0).getString("uuid"))
      }
    }
  }

  @Test
  def testFillAndReplaceAndRetrieveAttachmentCell(implicit c: TestContext): Unit = {
    okTest {
      val column = Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "kind" -> "attachment",
            "name" -> "Downloads"
          )
        )
      )

      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"
      val putFile = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test PDF"),
        "description" -> Json.obj("de-DE" -> "A description about that PDF.")
      )

      for {
        tableId <- createDefaultTable()

        columnId <- sendRequest("POST", s"/tables/$tableId/columns", column)
          .map(_.getJsonArray("columns").get[JsonObject](0).getInteger("id"))

        rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))

        fileUuid1 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid1", putFile)
        fileUuid2 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid2", putFile)
        fileUuid3 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid3", putFile)

        // Add attachment
        resultFill <- sendRequest(
          "PUT",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid1))
        )

        // Retrieve row with attachment
        resultRetrieve <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

        // Replace attachments with order
        resultReplace <- sendRequest(
          "PUT",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj(
            "value" -> Json.arr(
              Json.obj("uuid" -> fileUuid2, "ordering" -> 2),
              Json.obj("uuid" -> fileUuid3, "ordering" -> 1)
            )
          )
        )

        // Retrieve attachments after replace
        resultRetrieveAfterReplace <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        // Replace attachments without order
        resultReplaceWithoutOrder <- sendRequest(
          "PUT",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.arr(Json.obj("uuid" -> fileUuid2), Json.obj("uuid" -> fileUuid3)))
        )

        // Retrieve attachments after replace
        resultRetrieveAfterReplaceWithoutOrder <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        // Clear cell
        resultReplaceEmpty <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Json.arr()))

        // Retrieve attachments after replace
        resultRetrieveAfterReplaceEmpty <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        _ <- sendRequest("DELETE", s"/files/$fileUuid1")
        _ <- sendRequest("DELETE", s"/files/$fileUuid2")
        _ <- sendRequest("DELETE", s"/files/$fileUuid3")
      } yield {
        assertEquals(3, columnId)

        val uuid1 = resultFill.getJsonArray("value").getJsonObject(0).getString("uuid")
        assertNotNull(uuid1)
        assertJSONEquals(
          Json.obj(
            "ordering" -> 1,
            "folder" -> null,
            "uuid" -> uuid1,
            "title" -> Json.obj("de-DE" -> "Test PDF"),
            "url" -> Json.obj("de-DE" -> s"/files/$uuid1/de-DE/Scr%24en+Shot.pdf"),
            "description" -> Json.obj("de-DE" -> "A description about that PDF."),
            "externalName" -> Json.obj("de-DE" -> "Scr$en Shot.pdf"),
            "mimeType" -> Json.obj("de-DE" -> "application/pdf")
          ),
          resultFill.getJsonArray("value").getJsonObject(0)
        )

        assertEquals(
          fileUuid1,
          resultRetrieve.getJsonArray("values").getJsonArray(columnId - 1).getJsonObject(0).getString("uuid")
        )
        assertEquals(resultFill.getJsonArray("value"), resultRetrieve.getJsonArray("values").getJsonArray(columnId - 1))

        assertNotSame(
          resultRetrieve.getJsonArray("values").getJsonArray(columnId - 1),
          resultReplace.getJsonArray("value")
        )
        assertNotSame(uuid1, resultReplace.getJsonArray("value").getJsonObject(0).getString("uuid"))

        assertEquals(resultReplace, resultRetrieveAfterReplace)
        assertEquals(fileUuid3, resultRetrieveAfterReplace.getJsonArray("value").getJsonObject(0).getString("uuid"))
        assertEquals(fileUuid2, resultRetrieveAfterReplace.getJsonArray("value").getJsonObject(1).getString("uuid"))

        assertEquals(resultReplaceWithoutOrder, resultRetrieveAfterReplaceWithoutOrder)

        assertEquals(
          fileUuid2,
          resultRetrieveAfterReplaceWithoutOrder.getJsonArray("value").getJsonObject(0).getString("uuid")
        )
        assertEquals(
          fileUuid3,
          resultRetrieveAfterReplaceWithoutOrder.getJsonArray("value").getJsonObject(1).getString("uuid")
        )

        assertEquals("ok", resultReplaceEmpty.getString("status"))
        assertEquals(0, resultRetrieveAfterReplaceEmpty.getJsonArray("value").size())
        assertEquals(resultReplaceEmpty, resultRetrieveAfterReplaceEmpty)
      }
    }
  }

  @Test
  def testAddAttachmentWithMalformedUUID(implicit c: TestContext): Unit = {
    exceptionTest("error.arguments") {
      val column = Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "kind" -> "attachment",
            "name" -> "Downloads"
          )
        )
      )

      for {
        tableId <- createDefaultTable()
        columnId <- sendRequest("POST", s"/tables/$tableId/columns", column)
          .map(_.getJsonArray("columns").get[JsonObject](0).getInteger("id"))
        rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))

        // Add attachment with malformed uuid
        resultFill <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.obj("uuid" -> "this-is-not-an-uuid"))
        )
      } yield {
        resultFill
      }
    }
  }

  @Test
  def testUpdateAttachmentColumn(implicit c: TestContext): Unit = {
    okTest {
      val column = Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "kind" -> "attachment",
            "name" -> "Downloads"
          )
        )
      )

      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      val putFile = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test PDF"),
        "description" -> Json.obj("de-DE" -> "A description about that PDF.")
      )

      for {
        tableId <- createDefaultTable()

        columnId <- sendRequest("POST", s"/tables/$tableId/columns", column)
          .map(_.getJsonArray("columns").get[JsonObject](0).getInteger("id"))

        rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))

        fileUuid1 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid1", putFile)

        fileUuid2 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid2", putFile)

        // Add attachments
        resultFill1 <- sendRequest(
          "PATCH",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid1, "ordering" -> 1))
        )
        resultFill2 <- sendRequest(
          "PATCH",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid2))
        )

        // Retrieve attachments after fill
        resultRetrieveFill <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        // Update attachments
        resultUpdate1 <- sendRequest(
          "PATCH",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid1, "ordering" -> 2))
        )
        resultUpdate2 <- sendRequest(
          "PATCH",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid2, "ordering" -> 1))
        )

        // Retrieve attachments after update
        resultRetrieveUpdate <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        // Delete attachment
        _ <- sendRequest("DELETE", s"/tables/$tableId/columns/$columnId/rows/$rowId/attachment/$fileUuid2")

        // Retrieve attachment after delete
        resultRetrieveDelete <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        _ <- sendRequest("DELETE", s"/files/$fileUuid1")
        _ <- sendRequest("DELETE", s"/files/$fileUuid2")
      } yield {
        assertEquals(3, columnId)

        val uuid1 = resultFill1.getJsonArray("value").getJsonObject(0).getString("uuid")
        val uuid2 = resultFill2.getJsonArray("value").getJsonObject(0).getString("uuid")
        assertJSONEquals(
          Json.obj(
            "ordering" -> 1,
            "folder" -> null,
            "uuid" -> uuid1,
            "title" -> Json.obj("de-DE" -> "Test PDF"),
            "url" -> Json.obj("de-DE" -> s"/files/$uuid1/de-DE/Scr%24en+Shot.pdf"),
            "description" -> Json.obj("de-DE" -> "A description about that PDF."),
            "externalName" -> Json.obj("de-DE" -> "Scr$en Shot.pdf"),
            "mimeType" -> Json.obj("de-DE" -> "application/pdf")
          ),
          resultFill1.getJsonArray("value").getJsonObject(0)
        )
        assertJSONEquals(
          Json.obj(
            "folder" -> null,
            "uuid" -> uuid2,
            "title" -> Json.obj("de-DE" -> "Test PDF"),
            "url" -> Json.obj("de-DE" -> s"/files/$uuid2/de-DE/Scr%24en+Shot.pdf"),
            "description" -> Json.obj("de-DE" -> "A description about that PDF."),
            "externalName" -> Json.obj("de-DE" -> "Scr$en Shot.pdf"),
            "mimeType" -> Json.obj("de-DE" -> "application/pdf")
          ),
          resultFill2.getJsonArray("value").getJsonObject(0)
        )

        assertEquals(fileUuid1, resultRetrieveFill.getJsonArray("value").get[JsonObject](0).getString("uuid"))
        assertEquals(fileUuid2, resultRetrieveFill.getJsonArray("value").get[JsonObject](1).getString("uuid"))

        assertEquals(fileUuid2, resultRetrieveUpdate.getJsonArray("value").get[JsonObject](0).getString("uuid"))
        assertEquals(fileUuid1, resultRetrieveUpdate.getJsonArray("value").get[JsonObject](1).getString("uuid"))

        assertEquals(fileUuid1, resultRetrieveDelete.getJsonArray("value").get[JsonObject](0).getString("uuid"))
      }
    }
  }

  @Test
  def testDeleteAllAttachmentsFromCell(implicit c: TestContext): Unit = {
    okTest {
      val columns = RequestCreation
        .Columns()
        .add(RequestCreation.AttachmentCol("Downloads"))
        .getJson

      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      val putFile = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test PDF"),
        "description" -> Json.obj("de-DE" -> "A description about that PDF.")
      )

      for {
        tableId <- createDefaultTable()

        columnId <- sendRequest("POST", s"/tables/$tableId/columns", columns)
          .map(_.getJsonArray("columns").get[JsonObject](0).getInteger("id"))

        rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))

        fileUuid1 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid1", putFile)

        fileUuid2 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid2", putFile)

        // Add attachments
        resultFill1 <- sendRequest(
          "PATCH",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid1, "ordering" -> 1))
        )
        resultFill2 <- sendRequest(
          "PATCH",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid2))
        )

        // Retrieve attachments after fill
        resultRetrieveFill <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        // Delete attachments
        _ <- sendRequest("DELETE", s"/tables/$tableId/columns/$columnId/rows/$rowId/attachment/$fileUuid1")
        _ <- sendRequest("DELETE", s"/tables/$tableId/columns/$columnId/rows/$rowId/attachment/$fileUuid2")

        // Retrieve empty attachment cell after delete
        resultRetrieveDelete <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        _ <- sendRequest("DELETE", s"/files/$fileUuid1")
        _ <- sendRequest("DELETE", s"/files/$fileUuid2")
      } yield {
        assertEquals(3, columnId)

        val uuid1 = resultFill1.getJsonArray("value").getJsonObject(0).getString("uuid")
        val uuid2 = resultFill2.getJsonArray("value").getJsonObject(0).getString("uuid")
        assertJSONEquals(
          Json.obj(
            "ordering" -> 1,
            "folder" -> null,
            "uuid" -> uuid1,
            "title" -> Json.obj("de-DE" -> "Test PDF"),
            "url" -> Json.obj("de-DE" -> s"/files/$uuid1/de-DE/Scr%24en+Shot.pdf"),
            "description" -> Json.obj("de-DE" -> "A description about that PDF."),
            "externalName" -> Json.obj("de-DE" -> "Scr$en Shot.pdf"),
            "mimeType" -> Json.obj("de-DE" -> "application/pdf")
          ),
          resultFill1.getJsonArray("value").getJsonObject(0)
        )
        assertJSONEquals(
          Json.obj(
            "folder" -> null,
            "uuid" -> uuid2,
            "title" -> Json.obj("de-DE" -> "Test PDF"),
            "url" -> Json.obj("de-DE" -> s"/files/$uuid2/de-DE/Scr%24en+Shot.pdf"),
            "description" -> Json.obj("de-DE" -> "A description about that PDF."),
            "externalName" -> Json.obj("de-DE" -> "Scr$en Shot.pdf"),
            "mimeType" -> Json.obj("de-DE" -> "application/pdf")
          ),
          resultFill2.getJsonArray("value").getJsonObject(0)
        )

        assertEquals(fileUuid1, resultRetrieveFill.getJsonArray("value").get[JsonObject](0).getString("uuid"))
        assertEquals(fileUuid2, resultRetrieveFill.getJsonArray("value").get[JsonObject](1).getString("uuid"))

        assertEquals(0, resultRetrieveDelete.getJsonArray("value").size())
      }
    }
  }

  @Test
  def testDeleteLinkedFiles(implicit c: TestContext): Unit = {
    okTest {
      val columns = RequestCreation
        .Columns()
        .add(RequestCreation.AttachmentCol("Downloads"))
        .getJson

      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      val putFile = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test PDF"),
        "description" -> Json.obj("de-DE" -> "A description about that PDF.")
      )

      for {
        tableId <- createDefaultTable()

        columnId <- sendRequest("POST", s"/tables/$tableId/columns", columns)
          .map(_.getJsonArray("columns").get[JsonObject](0).getInteger("id"))

        rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))

        fileUuid1 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid1", putFile)

        fileUuid2 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid2", putFile)

        // Add attachments
        resultFill1 <- sendRequest(
          "PATCH",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid1, "ordering" -> 1))
        )
        resultFill2 <- sendRequest(
          "PATCH",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid2))
        )

        // Retrieve attachments after fill
        resultRetrieveFill <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        // Delete files
        _ <- sendRequest("DELETE", s"/files/$fileUuid1")
        _ <- sendRequest("DELETE", s"/files/$fileUuid2")

        // Retrieve empty attachment cell after delete
        resultRetrieveDelete <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")
      } yield {
        assertEquals(3, columnId)

        val uuid1 = resultFill1.getJsonArray("value").getJsonObject(0).getString("uuid")
        val uuid2 = resultFill2.getJsonArray("value").getJsonObject(0).getString("uuid")
        assertJSONEquals(
          Json.obj(
            "ordering" -> 1,
            "folder" -> null,
            "uuid" -> uuid1,
            "title" -> Json.obj("de-DE" -> "Test PDF"),
            "url" -> Json.obj("de-DE" -> s"/files/$uuid1/de-DE/Scr%24en+Shot.pdf"),
            "description" -> Json.obj("de-DE" -> "A description about that PDF."),
            "externalName" -> Json.obj("de-DE" -> "Scr$en Shot.pdf"),
            "mimeType" -> Json.obj("de-DE" -> "application/pdf")
          ),
          resultFill1.getJsonArray("value").getJsonObject(0)
        )
        assertJSONEquals(
          Json.obj(
            "folder" -> null,
            "uuid" -> uuid2,
            "title" -> Json.obj("de-DE" -> "Test PDF"),
            "url" -> Json.obj("de-DE" -> s"/files/$uuid2/de-DE/Scr%24en+Shot.pdf"),
            "description" -> Json.obj("de-DE" -> "A description about that PDF."),
            "externalName" -> Json.obj("de-DE" -> "Scr$en Shot.pdf"),
            "mimeType" -> Json.obj("de-DE" -> "application/pdf")
          ),
          resultFill2.getJsonArray("value").getJsonObject(0)
        )

        assertEquals(fileUuid1, resultRetrieveFill.getJsonArray("value").get[JsonObject](0).getString("uuid"))
        assertEquals(fileUuid2, resultRetrieveFill.getJsonArray("value").get[JsonObject](1).getString("uuid"))

        assertEquals(0, resultRetrieveDelete.getJsonArray("value").size())
      }
    }
  }

  @Test
  def testRetrieveAttachmentCellAfterReplacingFile(implicit c: TestContext): Unit = {
    okTest {
      val column = Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "kind" -> "attachment",
            "name" -> "Downloads"
          )
        )
      )

      val fileName = "Scr$en Shot.pdf"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"
      val putFile = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test PDF"),
        "description" -> Json.obj("de-DE" -> "A description about that PDF.")
      )

      val fileName2 = "Screen.Shot.png"
      val filePath2 = s"/com/campudus/tableaux/uploads/$fileName2"
      val mimetype2 = "image/png"

      for {
        tableId <- createDefaultTable()

        // Create attachment column
        columnId <- sendRequest("POST", s"/tables/$tableId/columns", column)
          .map(_.getJsonArray("columns").get[JsonObject](0).getInteger("id"))

        // Create new empty row
        rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))

        // Upload file
        fileUuid <- createFile("de-DE", filePath, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid", putFile)

        // Add attachment
        resultFill <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid))
        )

        // Retrieve attachment
        resultRetrieve1 <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        // Replace file
        _ <- replaceFile(fileUuid, "de-DE", filePath2, mimetype2)

        // Retrieve attachment after replace file
        resultRetrieve2 <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        _ <- sendRequest("DELETE", s"/files/$fileUuid")
      } yield {
        assertEquals(3, columnId)

        val uuid = resultFill.getJsonArray("value").getJsonObject(0).getString("uuid")
        assertNotNull(uuid)
        assertJSONEquals(
          Json.obj(
            "ordering" -> 1,
            "folder" -> null,
            "uuid" -> uuid,
            "title" -> Json.obj("de-DE" -> "Test PDF"),
            "url" -> Json.obj("de-DE" -> s"/files/$uuid/de-DE/Scr%24en+Shot.pdf"),
            "description" -> Json.obj("de-DE" -> "A description about that PDF."),
            "externalName" -> Json.obj("de-DE" -> "Scr$en Shot.pdf"),
            "mimeType" -> Json.obj("de-DE" -> "application/pdf")
          ),
          resultFill.getJsonArray("value").getJsonObject(0)
        )

        val attachment1 = resultRetrieve1.getJsonArray("value").get[JsonObject](0)
        val attachment2 = resultRetrieve2.getJsonArray("value").get[JsonObject](0)

        assertEquals(fileUuid, attachment1.getString("uuid"))
        assertEquals(fileUuid, attachment2.getString("uuid"))

        assertNotSame(resultRetrieve1, resultRetrieve2)

        assertEquals(attachment1.getJsonObject("title"), attachment2.getJsonObject("title"))
        assertEquals(attachment1.getJsonObject("description"), attachment2.getJsonObject("description"))

        assertEquals(Json.obj("de-DE" -> "Screen.Shot.png"), attachment2.getJsonObject("externalName"))
      }
    }
  }

  @Test
  def testFillAttachmentCellWithUUIDOnly(implicit c: TestContext): Unit = {
    okTest {
      val column = Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "kind" -> "attachment",
            "name" -> "Downloads"
          )
        )
      )

      val fileName = "Scr$en Shot.pdf"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"
      val putFile = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test PDF"),
        "description" -> Json.obj("de-DE" -> "A description about that PDF.")
      )

      for {
        tableId <- createDefaultTable()

        columnId <- sendRequest("POST", s"/tables/$tableId/columns", column)
          .map(_.getJsonArray("columns").get[JsonObject](0).getInteger("id"))

        rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))

        fileUuid1 <- createFile("de-DE", filePath, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid1", putFile)

        fileUuid2 <- createFile("de-DE", filePath, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid2", putFile)

        // Add two attachments with POST
        resultFillTwo <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.arr(fileUuid1, fileUuid2))
        )

        // Clear cell
        _ <- sendRequest("DELETE", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        // Add two attachments the other way aroung
        resultFillTwoOtherWayAround <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.arr(fileUuid2, fileUuid1))
        )

        // Clear cell
        _ <- sendRequest("DELETE", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        // Add one attachment
        resultFillOne <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj("value" -> Json.arr(fileUuid1))
        )

        resultFillOnePut <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> fileUuid2))

        _ <- sendRequest("DELETE", s"/files/$fileUuid1")
        _ <- sendRequest("DELETE", s"/files/$fileUuid2")
      } yield {
        assertEquals(3, columnId)

        assertEquals(1, resultFillOne.getJsonArray("value").size())
        assertEquals(1, resultFillOnePut.getJsonArray("value").size())

        assertEquals(2, resultFillTwo.getJsonArray("value").size())

        assertEquals(fileUuid1, resultFillOne.getJsonArray("value").getJsonObject(0).getString("uuid"))

        assertEquals(fileUuid2, resultFillOnePut.getJsonArray("value").getJsonObject(0).getString("uuid"))

        assertEquals(fileUuid1, resultFillTwo.getJsonArray("value").getJsonObject(0).getString("uuid"))
        assertEquals(fileUuid2, resultFillTwo.getJsonArray("value").getJsonObject(1).getString("uuid"))

        assertEquals(fileUuid2, resultFillTwoOtherWayAround.getJsonArray("value").getJsonObject(0).getString("uuid"))
        assertEquals(fileUuid1, resultFillTwoOtherWayAround.getJsonArray("value").getJsonObject(1).getString("uuid"))
      }
    }
  }

  @Test
  def testAttachmentdependentRowCount(implicit c: TestContext): Unit = {
    okTest {
      val column = Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "kind" -> "attachment",
            "name" -> "Downloads"
          )
        )
      )

      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"
      val putFile = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test PDF"),
        "description" -> Json.obj("de-DE" -> "A description about that PDF.")
      )

      for {
        tableId <- createDefaultTable()

        columnId <- sendRequest("POST", s"/tables/$tableId/columns", column)
          .map(_.getJsonArray("columns").get[JsonObject](0).getInteger("id"))

        rowId1 <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))
        rowId2 <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))
        rowId3 <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))

        fileUuid1 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid1", putFile)
        fileUuid2 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid2", putFile)

        // Add attachment to multiple rows
        resultRow1 <- sendRequest(
          "PUT",
          s"/tables/$tableId/columns/$columnId/rows/$rowId1",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid1))
        )
        resultRow2 <- sendRequest(
          "PUT",
          s"/tables/$tableId/columns/$columnId/rows/$rowId2",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid1))
        )
        resultRow3 <- sendRequest(
          "PUT",
          s"/tables/$tableId/columns/$columnId/rows/$rowId3",
          Json.obj("value" -> Json.obj("uuid" -> fileUuid1))
        )

        // Retrieve file
        resultFile1 <- sendRequest("GET", s"/files/$fileUuid1")
        resultFile2 <- sendRequest("GET", s"/files/$fileUuid2")

        // Delete file
        _ <- sendRequest("DELETE", s"/files/$fileUuid1")
      } yield {
        assertEquals(1, resultRow1.getJsonArray("value").getJsonObject(0).getNumber("dependentRowCount"))
        assertEquals(2, resultRow2.getJsonArray("value").getJsonObject(0).getNumber("dependentRowCount"))
        assertEquals(3, resultRow3.getJsonArray("value").getJsonObject(0).getNumber("dependentRowCount"))
        assertEquals(3, resultFile1.getNumber("dependentRowCount"))
        assertEquals(0, resultFile2.getNumber("dependentRowCount"))
      }
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class FolderTest extends MediaTestBase {

  @Test
  def testRetrieveRootFolder(implicit c: TestContext): Unit = okTest {
    for {
      rootFolder <- sendRequest("GET", "/folders?langtag=de-DE")
    } yield {
      assertEquals(null, rootFolder.getInteger("id"))
      assertEquals("root", rootFolder.getString("name"))
      assertNull(rootFolder.getString("parentId"))
    }
  }

  @Test
  def testCreateAndRetrieveFolder(implicit c: TestContext): Unit = {
    okTest {

      def createFolderPutJson(name: String): JsonObject = {
        Json.obj("name" -> name, "description" -> "Test Description", "parentId" -> null)
      }

      for {
        folderId <- sendRequest("POST", s"/folders", createFolderPutJson("Test")).map(_.getInteger("id"))

        folder <- sendRequest("GET", s"/folders/$folderId?langtag=de-DE")

        _ <- sendRequest("PUT", s"/folders/$folderId", createFolderPutJson("Update")).map(_.getInteger("id"))

        updatedFolder <- sendRequest("GET", s"/folders/$folderId?langtag=de-DE")
      } yield {
        assertEquals("Test", folder.getString("name"))
        assertEquals("Update", updatedFolder.getString("name"))
      }
    }
  }

  @Test
  def testRetrieveFolderWithoutLangtag(implicit c: TestContext): Unit = exceptionTest("error.param.notfound") {
    sendRequest("GET", s"/folders/1")
  }

  @Test
  def testCreateAndRetrieveFolderWithParents(implicit c: TestContext): Unit = {
    okTest {

      def createFolderPutJson(name: String, parentId: Option[Int]): JsonObject = {
        Json.obj("name" -> name, "description" -> "Test Description", "parentId" -> parentId.orNull)
      }

      for {
        rootId <- sendRequest("POST", s"/folders", createFolderPutJson("Test", None)).map(_.getInteger("id"))
        rootSubId <- sendRequest("POST", s"/folders", createFolderPutJson("Test", Some(rootId)))
          .map(_.getInteger("id"))
        rootSubSubId <- sendRequest("POST", s"/folders", createFolderPutJson("Test", Some(rootSubId)))
          .map(_.getInteger("id"))
        rootSubSubSubId <- sendRequest("POST", s"/folders", createFolderPutJson("Test", Some(rootSubSubId)))
          .map(_.getInteger("id"))

        rootSubSubSubSub1Id <- sendRequest("POST", s"/folders", createFolderPutJson("Test 1", Some(rootSubSubSubId)))
          .map(_.getInteger("id"))
        rootSubSubSubSub2Id <- sendRequest("POST", s"/folders", createFolderPutJson("Test 2", Some(rootSubSubSubId)))
          .map(_.getInteger("id"))

        sub1 <- sendRequest("GET", s"/folders/$rootSubSubSubSub1Id?langtag=de-DE")
        sub2 <- sendRequest("GET", s"/folders/$rootSubSubSubSub2Id?langtag=de-DE")
      } yield {
        assertEquals(Json.arr(rootId, rootSubId, rootSubSubId, rootSubSubSubId), sub1.getJsonArray("parentIds"))
        assertEquals(Json.arr(rootId, rootSubId, rootSubSubId, rootSubSubSubId), sub2.getJsonArray("parentIds"))

        assertEquals(sub1.getJsonArray("parentIds"), sub2.getJsonArray("parentIds"))

        // for compatibility reasons
        assertEquals(sub1.getInteger("parentId"), sub2.getInteger("parentId"))
      }
    }
  }

  @Test
  def testCreateAndRetrieveDeepFolderHierarchy(implicit c: TestContext): Unit = {
    okTest {

      def createFolderPutJson(name: String, parentId: Option[Int]): JsonObject = {
        Json.obj("name" -> name, "description" -> "Test Description", "parentId" -> parentId.orNull)
      }

      for {
        rootId <- sendRequest("POST", s"/folders", createFolderPutJson("root", None)).map(_.getInteger("id"))
        depthId_02 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-02", Some(rootId)))
          .map(_.getInteger("id"))
        depthId_03 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-03", Some(depthId_02)))
          .map(_.getInteger("id"))
        depthId_04 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-04", Some(depthId_03)))
          .map(_.getInteger("id"))
        depthId_05 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-05", Some(depthId_04)))
          .map(_.getInteger("id"))
        depthId_06 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-06", Some(depthId_05)))
          .map(_.getInteger("id"))
        depthId_07 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-07", Some(depthId_06)))
          .map(_.getInteger("id"))
        depthId_08 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-08", Some(depthId_07)))
          .map(_.getInteger("id"))
        depthId_09 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-09", Some(depthId_08)))
          .map(_.getInteger("id"))
        depthId_10 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-10", Some(depthId_09)))
          .map(_.getInteger("id"))
        depthId_11 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-11", Some(depthId_10)))
          .map(_.getInteger("id"))
        depthId_12 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-12", Some(depthId_11)))
          .map(_.getInteger("id"))
        depthId_13 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-13", Some(depthId_12)))
          .map(_.getInteger("id"))
        depthId_14 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-14", Some(depthId_13)))
          .map(_.getInteger("id"))
        depthId_15 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-15", Some(depthId_14)))
          .map(_.getInteger("id"))
        depthId_16 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-16", Some(depthId_15)))
          .map(_.getInteger("id"))
        depthId_17 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-17", Some(depthId_16)))
          .map(_.getInteger("id"))
        depthId_18 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-18", Some(depthId_17)))
          .map(_.getInteger("id"))
        depthId_19 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-19", Some(depthId_18)))
          .map(_.getInteger("id"))
        depthId_20 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-20", Some(depthId_19)))
          .map(_.getInteger("id"))
        depthId_21 <- sendRequest("POST", s"/folders", createFolderPutJson("depth-21", Some(depthId_20)))
          .map(_.getInteger("id"))

        sub21 <- sendRequest("GET", s"/folders/$depthId_21?langtag=de-DE")
      } yield {
        assertEquals(
          Json.arr(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
          sub21.getJsonArray("parentIds")
        )
      }
    }
  }

  @Test
  def testCreateFolderWithDuplicateName(implicit c: TestContext): Unit = {
    exceptionTest("error.request.unique.foldername") {

      def createFolderPutJson(name: String): JsonObject = {
        Json.obj("name" -> name, "description" -> "Test Description", "parentId" -> null)
      }

      for {
        _ <- sendRequest("POST", s"/folders", createFolderPutJson("Test"))
        _ <- sendRequest("POST", s"/folders", createFolderPutJson("Test"))
      } yield ()
    }
  }

  @Test
  def testCreateFolderWithDuplicateNameAndParent(implicit c: TestContext): Unit = {
    exceptionTest("error.request.unique.foldername") {

      def createFolderPutJson(name: String, parentId: Option[FolderId]): JsonObject = {
        Json.obj("name" -> name, "description" -> "Test Description", "parentId" -> parentId.orNull)
      }

      for {
        folderId <- sendRequest("POST", s"/folders", createFolderPutJson("Test", None)).map(_.getLong("id"))

        _ <- sendRequest("POST", s"/folders", createFolderPutJson("Test", Some(folderId)))
        _ <- sendRequest("POST", s"/folders", createFolderPutJson("Test", Some(folderId)))
      } yield ()
    }
  }

  @Test
  def testChangeFolderWithDuplicateName(implicit c: TestContext): Unit = {
    exceptionTest("error.request.unique.foldername") {

      def createFolderPutJson(name: String, parentId: Option[FolderId]): JsonObject = {
        Json.obj("name" -> name, "description" -> "Test Description", "parentId" -> parentId.orNull)
      }

      for {
        _ <- sendRequest("POST", s"/folders", createFolderPutJson("Test 1", None))
        folderId <- sendRequest("POST", s"/folders", createFolderPutJson("Test 2", None)).map(_.getLong("id"))

        _ <- sendRequest("PUT", s"/folders/$folderId", createFolderPutJson("Test 1", None))
      } yield ()
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class FileTest extends MediaTestBase {

  @Test
  def testFileUploadWithUnicodeFilenameAndUnicodeMetaInformation(implicit c: TestContext): Unit = okTest {
    val filePath = "/com/campudus/tableaux/uploads/Screen Shöt.jpg"
    val mimetype = "image/jpeg"

    val meta = Json.obj(
      "title" -> Json.obj(
        "zh_CN" -> "情暮夜告書究"
      ),
      "description" -> Json.obj(
        "zh_CN" -> "情暮夜告書究情暮夜告書究"
      )
    )

    for {
      file <- sendRequest("POST", "/files", meta)

      uploadedFile <- replaceFile(file.getString("uuid"), "zh_CN", filePath, mimetype)
      puttedFile <- sendRequest("PUT", s"/files/${file.getString("uuid")}", meta)
      deletedFile <- sendRequest("DELETE", s"/files/${file.getString("uuid")}")
    } yield {
      assertEquals(true, file.getBoolean("tmp"))

      assertEquals(meta.getJsonObject("title"), uploadedFile.getJsonObject("title"))
      assertEquals(Path(filePath).name, uploadedFile.getJsonObject("externalName").getString("zh_CN"))
      assertEquals(false, uploadedFile.containsKey("tmp"))

      assertEquals(false, puttedFile.containsKey("tmp"))
      assertEquals(meta.getJsonObject("name"), puttedFile.getJsonObject("name"))
      assertEquals(meta.getJsonObject("description"), puttedFile.getJsonObject("description"))

      assertEquals(uploadedFile.getJsonObject("externalName"), puttedFile.getJsonObject("externalName"))

      // We need to remove url because DELETE doesn't return ExtendedFile
      puttedFile.remove("url")
      assertEquals(puttedFile, deletedFile)
    }
  }

  @Test
  def testFileUploadAndFileDownload(implicit c: TestContext): Unit = {
    okTest {
      val fileName = "Scr$en Shot.pdf"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"
      val size = vertx.fileSystem.propsBlocking(getClass.getResource(filePath).toURI.getPath).size()

      val meta = Json.obj(
        "title" -> Json.obj(
          "de-DE" -> "Test PDF"
        ),
        "description" -> Json.obj(
          "de-DE" -> "A description about that PDF."
        )
      )

      for {
        file <- sendRequest("POST", "/files", meta)

        uploadedFile <- replaceFile(file.getString("uuid"), "de-DE", filePath, mimetype)
        puttedFile <- sendRequest("PUT", s"/files/${file.getString("uuid")}", meta)

        file <- sendRequest("GET", s"/files/${file.getString("uuid")}")

        _ <- futurify { p: Promise[Unit] =>
          {
            val url = file.getJsonObject("url").getString("de-DE")

            httpRequest(
              "GET",
              s"$url",
              { (client: HttpClient, resp: HttpClientResponse) =>
                {
                  assertEquals(200, resp.statusCode())

                  assertEquals("Should get the correct MIME type", Some(mimetype), resp.getHeader("content-type"))
                  assertEquals(
                    "Should get the correct content length",
                    Some(size.toString),
                    resp.getHeader("content-length")
                  )

                  resp.bodyHandler { buf: Buffer =>
                    client.close()

                    assertEquals("Should get the same size back as the file really is", size, buf.length())
                    p.success(())
                  }
                }
              },
              { (client: HttpClient, x: Throwable) =>
                client.close()
                c.fail(x)
                p.failure(x)
              },
              None
            ).end()
          }
        }

        _ <- sendRequest("DELETE", s"/files/${file.getString("uuid")}")
      } yield {
        assertEquals(meta.getJsonObject("title"), file.getJsonObject("title"))
        assertEquals(meta.getJsonObject("description"), file.getJsonObject("description"))
      }
    }
  }

  @Test
  def testFileUploadAndFileDownloadWithShortLangtags(implicit c: TestContext): Unit = {
    okTest {
      val fileName = "Scr$en Shot.pdf"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"
      val size = vertx.fileSystem.propsBlocking(getClass.getResource(filePath).toURI.getPath).size()

      val meta = Json.obj(
        "title" -> Json.obj(
          "de" -> "Test PDF"
        ),
        "description" -> Json.obj(
          "de" -> "A description about that PDF."
        )
      )

      for {
        file <- sendRequest("POST", "/files", meta)

        uploadedFile <- replaceFile(file.getString("uuid"), "de", filePath, mimetype)
        puttedFile <- sendRequest("PUT", s"/files/${file.getString("uuid")}", meta)

        file <- sendRequest("GET", s"/files/${file.getString("uuid")}")

        _ <- futurify { p: Promise[Unit] =>
          {
            val url = file.getJsonObject("url").getString("de")

            httpRequest(
              "GET",
              s"$url",
              { (client: HttpClient, resp: HttpClientResponse) =>
                {
                  assertEquals(200, resp.statusCode())

                  assertEquals("Should get the correct MIME type", Some(mimetype), resp.getHeader("content-type"))
                  assertEquals(
                    "Should get the correct content length",
                    Some(size.toString),
                    resp.getHeader("content-length")
                  )

                  resp.bodyHandler { buf: Buffer =>
                    client.close()

                    assertEquals("Should get the same size back as the file really is", size, buf.length())
                    p.success(())
                  }
                }
              },
              { (client: HttpClient, x: Throwable) =>
                client.close()
                c.fail(x)
                p.failure(x)
              },
              None
            ).end()
          }
        }

        _ <- sendRequest("DELETE", s"/files/${file.getString("uuid")}")
      } yield {
        assertEquals(meta.getJsonObject("title"), file.getJsonObject("title"))
        assertEquals(meta.getJsonObject("description"), file.getJsonObject("description"))
      }
    }
  }

  @Test
  def testFileWithMultiLanguageMetaInformation(implicit c: TestContext): Unit = {
    okTest {
      val fileName = "Scr$en Shot.pdf"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      val putOne = Json.obj(
        "title" -> Json.obj(
          "de-DE" -> "Ein schöner deutscher Titel."
        ),
        "description" -> Json.obj(
          "de-DE" -> "Und hier folgt eine tolle hochdeutsche Beschreibung."
        )
      )

      val putTwo = Json.obj(
        "title" -> Json.obj(
          "en-GB" -> "A beautiful German title."
        ),
        "description" -> Json.obj(
          "en-GB" -> "And here is a great High German description."
        )
      )

      for {
        file <- sendRequest("POST", "/files", putOne)

        uploadedFile <- replaceFile(file.getString("uuid"), "de-DE", filePath, mimetype)

        _ <- sendRequest("PUT", s"/files/${file.getString("uuid")}", putOne)
        _ <- sendRequest("PUT", s"/files/${file.getString("uuid")}", putTwo)

        fileRequested <- sendRequest("GET", s"/files/${file.getString("uuid")}")

        _ <- replaceFile(file.getString("uuid"), "en-GB", filePath, mimetype)
        fileReplaced <- sendRequest("GET", s"/files/${file.getString("uuid")}")

        _ <- replaceFile(file.getString("uuid"), "de-DE", filePath, mimetype)
        fileReplaced2 <- sendRequest("GET", s"/files/${file.getString("uuid")}")

        deletedEnGB <- sendRequest("DELETE", s"/files/${file.getString("uuid")}/en-GB")

        _ <- sendRequest("DELETE", s"/files/${file.getString("uuid")}")
      } yield {
        assertEquals(
          putOne.getJsonObject("title").mergeIn(putTwo.getJsonObject("title")),
          fileRequested.getJsonObject("title")
        )

        assertFalse(file.getJsonObject("url").containsKey("en-GB"))
        assertTrue(
          fileReplaced
            .getJsonObject("url")
            .containsKey("en-GB") && fileReplaced.getJsonObject("url").getString("en-GB") != null
        )

        assertNotSame(
          fileReplaced.getJsonObject("internalName").getString("de-DE"),
          fileReplaced2.getJsonObject("internalName").getString("de-DE")
        )

        assertFalse(deletedEnGB.getJsonObject("internalName").containsKey("en-GB"))
      }
    }
  }

  @Test
  def testRecusiveDeleteOfFoldersWithFiles(implicit c: TestContext): Unit = {
    okTest {
      val filePath = "/com/campudus/tableaux/uploads/Screen Shöt.jpg"
      val mimetype = "image/jpeg"

      def createFolderPutJson(name: String, parentId: Option[Int] = None): JsonObject = {
        Json.obj("name" -> name, "description" -> "Test Description", "parentId" -> parentId.orNull)
      }

      def createFilePutJson(folder: Int): JsonObject = {
        Json.obj(
          "title" -> Json.obj("de-DE" -> "Test File"),
          "description" -> Json.obj("de-DE" -> "Test Description"),
          "folder" -> folder
        )
      }

      for {
        folder1 <- sendRequest("POST", s"/folders", createFolderPutJson("Test 1")).map(_.getInteger("id"))
        folder2 <- sendRequest("POST", s"/folders", createFolderPutJson("Test 2")).map(_.getInteger("id"))

        folder11 <- sendRequest("POST", s"/folders", createFolderPutJson("Test 1", Some(folder1)))
          .map(_.getInteger("id"))
        folder12 <- sendRequest("POST", s"/folders", createFolderPutJson("Test 2", Some(folder1)))
          .map(_.getInteger("id"))

        folder21 <- sendRequest("POST", s"/folders", createFolderPutJson("Test 1", Some(folder2)))
          .map(_.getInteger("id"))
        folder22 <- sendRequest("POST", s"/folders", createFolderPutJson("Test 2", Some(folder2)))
          .map(_.getInteger("id"))

        file1 <- sendRequest("POST", "/files", createFilePutJson(folder11))
        file1Uuid = file1.getString("uuid")
        _ <- replaceFile(file1Uuid, "de-DE", filePath, mimetype)
        puttedFile1 <- sendRequest("PUT", s"/files/$file1Uuid", createFilePutJson(folder11))

        file2 <- sendRequest("POST", "/files", createFilePutJson(folder21))
        file2Uuid = file2.getString("uuid")
        _ <- replaceFile(file2Uuid, "de-DE", filePath, mimetype)
        puttedFile2 <- sendRequest("PUT", s"/files/$file2Uuid", createFilePutJson(folder21))

        deleteFolder1 <- sendRequest("DELETE", s"/folders/$folder1")
        deleteFolder2 <- sendRequest("DELETE", s"/folders/$folder2")
      } yield {
        assertEquals(folder1, deleteFolder1.getInteger("id"))
        assertEquals(folder2, deleteFolder2.getInteger("id"))

        assertEquals(Json.arr(folder1, folder11), file1.getJsonArray("folders"))
        assertEquals(Json.arr(folder2, folder21), file2.getJsonArray("folders"))
      }
    }
  }

  @Test
  def testDeleteTmpFile(implicit c: TestContext): Unit = okTest {
    val meta = Json.obj(
      "title" -> Json.obj(
        "en-GB" -> "A beautiful German title."
      ),
      "description" -> Json.obj(
        "en-GB" -> "And here is a great High German description."
      )
    )

    for {
      tmpFile <- sendRequest("POST", "/files", meta)
      _ <- sendRequest("GET", s"/files/${tmpFile.getString("uuid")}")
        .recoverWith({
          case TestCustomException(_, _, 404) =>
            Future.successful(())
          case e =>
            Future.failed(new Exception("Should be a 404"))
        })

      deletedFile <- sendRequest("DELETE", s"/files/${tmpFile.getString("uuid")}")
      _ <- sendRequest("GET", s"/files/${tmpFile.getString("uuid")}")
        .recoverWith({
          case TestCustomException(_, _, 404) =>
            Future.successful(())
          case e =>
            Future.failed(new Exception("Should be a 404"))
        })
    } yield {
      assertEquals(tmpFile.getString("uuid"), deletedFile.getString("uuid"))
    }
  }

  @Test
  def testDeletingAlreadyDeletedTmpFile(implicit c: TestContext): Unit = {
    okTest {
      val file = "/com/campudus/tableaux/uploads/Screen Shöt.jpg"
      val mimetype = "image/jpeg"

      val meta = Json.obj(
        "title" -> Json.obj(
          "en-GB" -> "A beautiful German title."
        ),
        "description" -> Json.obj(
          "en-GB" -> "And here is a great High German description."
        )
      )

      for {
        tmpFile <- sendRequest("POST", "/files", meta)
        uploadedFile <- replaceFile(tmpFile.getString("uuid"), "de-DE", file, mimetype)

        _ <- {
          val uploadsDirectory = tableauxConfig.uploadsDirectoryPath()

          val path = uploadsDirectory / Path(uploadedFile.getJsonObject("internalName").getString("de-DE"))

          // delete tmp file
          vertx
            .fileSystem()
            .deleteFuture(path.toString())
        }

        result <- sendRequest("DELETE", s"/files/${tmpFile.getString("uuid")}")
      } yield {
        assertEquals(tmpFile.getString("uuid"), result.getString("uuid"))
      }
    }
  }

  @Test
  def testMergeFiles(implicit c: TestContext): Unit = {
    okTest {
      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      val putFile1 = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test PDF 1"),
        "description" -> Json.obj("de-DE" -> "A description about that PDF. 1")
      )
      val putFile2 = Json.obj(
        "title" -> Json.obj("en-GB" -> "Test PDF 2"),
        "description" -> Json.obj("en-GB" -> "A description about that PDF. 2")
      )

      for {
        fileUuid1 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        fileAfterPut1 <- sendRequest("PUT", s"/files/$fileUuid1", putFile1)

        fileUuid2 <- createFile("en-GB", file, mimetype, None) map (_.getString("uuid"))
        fileAfterPut2 <- sendRequest("PUT", s"/files/$fileUuid2", putFile2)

        _ <- sendRequest("POST", s"/files/$fileUuid1/merge", Json.obj("mergeWith" -> fileUuid2, "langtag" -> "en-GB"))

        fileAfterMerge <- sendRequest("GET", s"/files/$fileUuid1")

        files <- sendRequest("GET", s"/folders?langtag=de-DE")

        _ <- sendRequest("DELETE", s"/files/$fileUuid1")
      } yield {
        assertEquals(1, files.getJsonArray("files", Json.emptyArr()).size())

        assertEquals(
          fileAfterPut1.getJsonObject("internalName").getString("de-DE"),
          fileAfterMerge.getJsonObject("internalName").getString("de-DE")
        )
        assertEquals(
          fileAfterPut2.getJsonObject("internalName").getString("en-GB"),
          fileAfterMerge.getJsonObject("internalName").getString("en-GB")
        )
      }
    }
  }

  @Test
  def testFileSorting(implicit c: TestContext): Unit = {
    okTest {
      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      val putFileA = Json.obj("externalName" -> Json.obj("de-DE" -> "A.pdf"))
      val putFileB = Json.obj("externalName" -> Json.obj("de-DE" -> "B.pdf"))
      val putFileC = Json.obj("externalName" -> Json.obj("de-DE" -> "C.pdf"))

      for {
        fileUuid1 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        fileAfterPut1 <- sendRequest("PUT", s"/files/$fileUuid1", putFileC)

        fileUuid2 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        fileAfterPut2 <- sendRequest("PUT", s"/files/$fileUuid2", putFileA)

        fileUuid3 <- createFile("de-DE", file, mimetype, None) map (_.getString("uuid"))
        fileAfterPut3 <- sendRequest("PUT", s"/files/$fileUuid3", putFileB)

        files <- sendRequest("GET", s"/folders?langtag=de-DE")

        _ <- sendRequest("DELETE", s"/files/$fileUuid1")
        _ <- sendRequest("DELETE", s"/files/$fileUuid2")
        _ <- sendRequest("DELETE", s"/files/$fileUuid3")
      } yield {
        assertEquals(3, files.getJsonArray("files", Json.emptyArr()).size())

        assertEquals(
          "A.pdf",
          files
            .getJsonArray("files", Json.emptyArr())
            .getJsonObject(0)
            .getJsonObject("externalName")
            .getString("de-DE")
        )
        assertEquals(
          "B.pdf",
          files
            .getJsonArray("files", Json.emptyArr())
            .getJsonObject(1)
            .getJsonObject("externalName")
            .getString("de-DE")
        )
        assertEquals(
          "C.pdf",
          files
            .getJsonArray("files", Json.emptyArr())
            .getJsonObject(2)
            .getJsonObject("externalName")
            .getString("de-DE")
        )

      }
    }
  }

  @Test
  def testChangeFileMetaInformation(implicit c: TestContext): Unit = {
    okTest {
      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      for {
        fileAfterCreate <- createFile("de-DE", file, mimetype, None)
        (fileUuid, internalNameDe) <- Future.successful(
          (fileAfterCreate.getString("uuid"), fileAfterCreate.getJsonObject("internalName").getString("de-DE"))
        )
        fileAfterUploadEn <- uploadFile("PUT", s"/files/$fileUuid/en-GB", file, mimetype)
        internalNameEn <- Future.successful(fileAfterUploadEn.getJsonObject("internalName").getString("en-GB"))
        fileAfterChange <- sendRequest(
          "PUT",
          s"/files/$fileUuid",
          Json.obj(
            "title" -> Json.obj(
              "de-DE" -> "A_de.pdf",
              "en-GB" -> "A_en.pdf"
            ),
            "description" -> Json.obj(
              "de-DE" -> "desc deutsch",
              "en-GB" -> "desc english"
            ),
            "externalName" -> Json.obj(
              "de-DE" -> "A_de.pdf",
              "en-GB" -> "A_en.pdf"
            ),
            "internalName" -> Json.obj(
              "de-DE" -> internalNameEn,
              "en-GB" -> internalNameDe
            ),
            "mimeType" -> Json.obj(
              "de-DE" -> "application/pdf",
              "en-GB" -> "application/pdf"
            )
          )
        )

        _ <- sendRequest("DELETE", s"/files/$fileUuid")
      } yield {
        assertEquals("A_de.pdf", fileAfterChange.getJsonObject("title").getString("de-DE"))
        assertEquals("desc deutsch", fileAfterChange.getJsonObject("description").getString("de-DE"))
        assertEquals("A_de.pdf", fileAfterChange.getJsonObject("externalName").getString("de-DE"))
        assertEquals(internalNameEn, fileAfterChange.getJsonObject("internalName").getString("de-DE"))
        assertEquals("application/pdf", fileAfterChange.getJsonObject("mimeType").getString("de-DE"))

        assertEquals("A_en.pdf", fileAfterChange.getJsonObject("title").getString("en-GB"))
        assertEquals("desc english", fileAfterChange.getJsonObject("description").getString("en-GB"))
        assertEquals("A_en.pdf", fileAfterChange.getJsonObject("externalName").getString("en-GB"))
        assertEquals(internalNameDe, fileAfterChange.getJsonObject("internalName").getString("en-GB"))
        assertEquals("application/pdf", fileAfterChange.getJsonObject("mimeType").getString("en-GB"))

      }
    }
  }

  @Test
  def testFailChangeFileMetaInformationWithPathInFile1(implicit c: TestContext): Unit = {
    exceptionTest("error.request.invalid") {
      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      for {
        fileAfterCreate <- createFile("de-DE", file, mimetype, None)
        (fileUuid, internalNameDe) <- Future.successful(
          (fileAfterCreate.getString("uuid"), fileAfterCreate.getJsonObject("internalName").getString("de-DE"))
        )
        exception <- sendRequest(
          "PUT",
          s"/files/$fileUuid",
          Json.obj(
            "title" -> Json.obj(
              "de-DE" -> "A_de.pdf"
            ),
            "description" -> Json.obj(
              "de-DE" -> "desc deutsch"
            ),
            "externalName" -> Json.obj(
              "de-DE" -> "A_de.pdf"
            ),
            "internalName" -> Json.obj(
              "de-DE" -> "../blablubb.config"
            ),
            "mimeType" -> Json.obj(
              "de-DE" -> "application/pdf"
            )
          )
        ) recover {
          case ex => ex
        }

        _ <- sendRequest("DELETE", s"/files/$fileUuid")
      } yield {
        exception match {
          case ex: Throwable => throw ex
        }
      }
    }
  }

  @Test
  def testFailChangeFileMetaInformationWithPathInFile2(implicit c: TestContext): Unit = {
    exceptionTest("error.request.invalid") {
      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      for {
        fileAfterCreate <- createFile("de-DE", file, mimetype, None)
        (fileUuid, internalNameDe) <- Future.successful(
          (fileAfterCreate.getString("uuid"), fileAfterCreate.getJsonObject("internalName").getString("de-DE"))
        )
        exception <- sendRequest(
          "PUT",
          s"/files/$fileUuid",
          Json.obj(
            "title" -> Json.obj(
              "de-DE" -> "A_de.pdf"
            ),
            "description" -> Json.obj(
              "de-DE" -> "desc deutsch"
            ),
            "externalName" -> Json.obj(
              "de-DE" -> "A_de.pdf"
            ),
            "internalName" -> Json.obj(
              "de-DE" -> "..\\bla\\blablubb.config"
            ),
            "mimeType" -> Json.obj(
              "de-DE" -> "application/pdf"
            )
          )
        ) recover {
          case ex => ex
        }

        _ <- sendRequest("DELETE", s"/files/$fileUuid")
      } yield {
        exception match {
          case ex: Throwable => throw ex
        }
      }
    }
  }

  @Test
  def testFailChangeFileMetaInformationWithInvalidFile1(implicit c: TestContext): Unit = {
    exceptionTest("error.request.invalid") {
      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      for {
        fileAfterCreate <- createFile("de-DE", file, mimetype, None)
        (fileUuid, internalNameDe) <- Future.successful(
          (fileAfterCreate.getString("uuid"), fileAfterCreate.getJsonObject("internalName").getString("de-DE"))
        )
        exception <- sendRequest(
          "PUT",
          s"/files/$fileUuid",
          Json.obj(
            "title" -> Json.obj(
              "de-DE" -> "A_de.pdf"
            ),
            "description" -> Json.obj(
              "de-DE" -> "desc deutsch"
            ),
            "externalName" -> Json.obj(
              "de-DE" -> "A_de.pdf"
            ),
            "internalName" -> Json.obj(
              "de-DE" -> ".."
            ),
            "mimeType" -> Json.obj(
              "de-DE" -> "application/pdf"
            )
          )
        ) recover {
          case ex => ex
        }

        _ <- sendRequest("DELETE", s"/files/$fileUuid")
      } yield {
        exception match {
          case ex: Throwable => throw ex
        }
      }
    }
  }

  @Test
  def testFailChangeFileMetaInformationWithInvalidFile2(implicit c: TestContext): Unit = {
    exceptionTest("error.request.invalid") {
      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      for {
        fileAfterCreate <- createFile("de-DE", file, mimetype, None)
        (fileUuid, internalNameDe) <- Future.successful(
          (fileAfterCreate.getString("uuid"), fileAfterCreate.getJsonObject("internalName").getString("de-DE"))
        )
        exception <- sendRequest(
          "PUT",
          s"/files/$fileUuid",
          Json.obj(
            "title" -> Json.obj(
              "de-DE" -> "A_de.pdf"
            ),
            "description" -> Json.obj(
              "de-DE" -> "desc deutsch"
            ),
            "externalName" -> Json.obj(
              "de-DE" -> "A_de.pdf"
            ),
            "internalName" -> Json.obj(
              "de-DE" -> "."
            ),
            "mimeType" -> Json.obj(
              "de-DE" -> "application/pdf"
            )
          )
        ) recover {
          case ex => ex
        }

        _ <- sendRequest("DELETE", s"/files/$fileUuid")
      } yield {
        exception match {
          case ex: Throwable => throw ex
        }
      }
    }
  }

  @Test
  def testChangeFileInternalnameToNull(implicit c: TestContext): Unit = {
    okTest {
      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      for {
        fileAfterCreate <- createFile("de-DE", file, mimetype, None)
        (fileUuid, internalNameDe) <- Future.successful(
          (fileAfterCreate.getString("uuid"), fileAfterCreate.getJsonObject("internalName").getString("de-DE"))
        )
        fileAfterChange <- sendRequest(
          "PUT",
          s"/files/$fileUuid",
          Json.obj(
            "title" -> Json.obj(
              "de-DE" -> "A_de.pdf"
            ),
            "description" -> Json.obj(
              "de-DE" -> "desc deutsch"
            ),
            "externalName" -> Json.obj(
              "de-DE" -> "A_de.pdf"
            ),
            "internalName" -> Json.obj(
              "de-DE" -> null
            ),
            "mimeType" -> Json.obj(
              "de-DE" -> null
            )
          )
        )

        _ <- sendRequest("DELETE", s"/files/$fileUuid")
      } yield {
        assertEquals("A_de.pdf", fileAfterChange.getJsonObject("title").getString("de-DE"))
        assertEquals("desc deutsch", fileAfterChange.getJsonObject("description").getString("de-DE"))
        assertEquals("A_de.pdf", fileAfterChange.getJsonObject("externalName").getString("de-DE"))
        assertEquals(null, fileAfterChange.getJsonObject("internalName").getString("de-DE"))
        assertEquals(null, fileAfterChange.getJsonObject("mimeType").getString("de-DE"))
      }
    }
  }

  @Test
  def testChangeFileInternalnameAndMimeType(implicit c: TestContext): Unit = {
    okTest {
      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      for {
        fileAfterCreate <- createFile("de-DE", file, mimetype, None)
        (fileUuid, internalNameDe) <- Future.successful(
          (fileAfterCreate.getString("uuid"), fileAfterCreate.getJsonObject("internalName").getString("de-DE"))
        )
        fileAfterChange <- sendRequest(
          "PUT",
          s"/files/$fileUuid",
          Json.obj(
            "title" -> Json.obj(
              "de-DE" -> "A_de.pdf"
            ),
            "description" -> Json.obj(
              "de-DE" -> "desc deutsch"
            ),
            "externalName" -> Json.obj(
              "de-DE" -> "A_de.pdf"
            ),
            "internalName" -> Json.obj(
              "de-DE" -> internalNameDe
            ),
            "mimeType" -> Json.obj(
              "de-DE" -> "text/plain"
            )
          )
        )

        _ <- sendRequest("DELETE", s"/files/$fileUuid")
      } yield {
        assertEquals("A_de.pdf", fileAfterChange.getJsonObject("title").getString("de-DE"))
        assertEquals("desc deutsch", fileAfterChange.getJsonObject("description").getString("de-DE"))
        assertEquals("A_de.pdf", fileAfterChange.getJsonObject("externalName").getString("de-DE"))
        assertEquals(internalNameDe, fileAfterChange.getJsonObject("internalName").getString("de-DE"))
        assertEquals("text/plain", fileAfterChange.getJsonObject("mimeType").getString("de-DE"))
      }
    }
  }
}
