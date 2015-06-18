package com.campudus.tableaux

import java.net.URLEncoder

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.{File, Folder}
import com.campudus.tableaux.database.model.{FileModel, FolderModel}
import com.campudus.tableaux.helper.FutureUtils
import org.junit.Test
import org.vertx.java.core.json.JsonObject
import org.vertx.scala.core.http.HttpClientRequest
import org.vertx.scala.core.json.Json
import org.vertx.scala.core.streams.Pump
import org.vertx.testtools.VertxAssert._

import scala.concurrent.{Future, Promise}

class MediaTest extends TableauxTestBase {

  def createFileModel(): FileModel = {
    val dbConnection = DatabaseConnection(tableauxConfig)

    new FileModel(dbConnection)
  }

  def createFolderModel(): FolderModel = {
    val dbConnection = DatabaseConnection(tableauxConfig)

    new FolderModel(dbConnection)
  }

  @Test
  def testFileModel(): Unit = okTest {
    val file = File("lulu", "lala", "llu")

    for {
      model <- Future.successful(createFileModel())

      tempFile1 <- model.add(file)
      tempFile2 <- model.add(file)

      sizeAfterAdd <- model.size()

      insertedFile1 <- model.update(tempFile1)
      insertedFile2 <- model.update(tempFile2)

      retrievedFile <- model.retrieve(insertedFile1.uuid.get)
      updatedFile <- model.update(File(retrievedFile.uuid.get, "blub", "flab", "test"))

      allFiles <- model.retrieveAll()

      size <- model.size()

      _ <- model.delete(insertedFile1)

      sizeAfterDeleteOne <- model.size()
    } yield {
      assertEquals(0, sizeAfterAdd)

      assertEquals(insertedFile1, retrievedFile)

      assert(retrievedFile.updatedAt.isDefined)

      assert(updatedFile.createdAt.isDefined)
      assert(updatedFile.updatedAt.isDefined)

      assert(updatedFile.updatedAt.get.isAfter(updatedFile.createdAt.get))

      assertEquals(2, allFiles.toList.size)

      assertEquals(2, size)
      assertEquals(1, sizeAfterDeleteOne)
    }
  }

  @Test
  def testFolderModel(): Unit = okTest {
    val folder = Folder(None, name = "hallo", description = "Test", None, None, None)

    for {
      model <- Future.successful(createFolderModel())

      insertedFolder1 <- model.add(folder)
      insertedFolder2 <- model.add(folder)

      retrievedFolder <- model.retrieve(insertedFolder1.id.get)
      updatedFolder <- model.update(Folder(retrievedFolder.id, name = "blub", description = "flab", None, None, None))

      folders <- model.retrieveAll()

      size <- model.size()
      _ <- model.delete(insertedFolder1)
      sizeAfterDelete <- model.size()
    } yield {
      assertEquals(insertedFolder1, retrievedFolder)

      assert(retrievedFolder.updatedAt.isEmpty)
      assert(updatedFolder.createdAt.isDefined)
      assert(updatedFolder.updatedAt.isDefined)

      assert(updatedFolder.updatedAt.get.isAfter(updatedFolder.createdAt.get))

      assertEquals(2, folders.toList.size)

      assertEquals(2, size)
      assertEquals(1, sizeAfterDelete)
    }
  }

  @Test
  def retrieveRootFolder(): Unit = okTest {

    for {
      rootFolder <- sendRequest("GET", "/folders")
    } yield {
      assertNull(rootFolder.getString("id"))
      assertEquals("root", rootFolder.getString("name"))
      assertNull(rootFolder.getString("parent"))
    }
  }

  @Test
  def uploadFileWithNonAsciiCharacterName(): Unit = okTest {
    val file = "/com/campudus/tableaux/uploads/Screen Shöt.jpg"
    val mimetype = "image/jpeg"

    val put = Json.obj("name" -> "情暮夜告書究", "description" -> "情暮夜告書究情暮夜告書究")

    for {
      uploadResponse <- uploadFile(file, mimetype)
      puttedFile <- sendRequestWithJson("PUT", put, s"/files/${uploadResponse.getString("uuid")}")
      deletedFile <- sendRequest("DELETE", s"/files/${uploadResponse.getString("uuid")}")
    } yield {
      assertEquals(true, uploadResponse.getBoolean("tmp"))
      assertEquals("Screen Shöt.jpg", uploadResponse.getString("name"))
      assertEquals(uploadResponse.getString("name"), uploadResponse.getString("filename"))

      assertEquals(false, puttedFile.containsField("tmp"))
      assertEquals(put.getString("name"), puttedFile.getString("name"))
      assertEquals(put.getString("description"), puttedFile.getString("description"))
      assertEquals(uploadResponse.getString("filename"), puttedFile.getString("filename"))

      assertEquals(puttedFile, deletedFile)
    }
  }

  @Test
  def retrieveFile(): Unit = okTest {
    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"
    val size = vertx.fileSystem.propsSync(getClass.getResource(file).toURI.getPath).size()

    val put = Json.obj("name" -> "Test PDF", "description" -> "A description about that PDF.")

    import FutureUtils._

    for {
      uploadResponse <- uploadFile(file, mimetype)
      _ <- sendRequestWithJson("PUT", put, s"/files/${uploadResponse.getString("uuid")}")
      request <- promisify { p: Promise[Unit] =>

        val url = s"/files/${uploadResponse.getString("uuid")}/" + URLEncoder.encode(fileName, "UTF-8")

        httpRequest("GET", url.toString, {
          resp =>
            assertEquals(200, resp.statusCode())

            assertEquals("Should get the correct MIME type", mimetype, resp.headers().get("content-type").get.head)
            assertEquals("Should get the correct content length", String.valueOf(size), resp.headers().get("content-length").get.head)

            resp.bodyHandler { buf =>
              assertEquals("Should get the same size back as the file really is", size, buf.length())
              p.success()
            }
        }).exceptionHandler({ ext =>
          fail(ext.toString)
          p.failure(ext)
        }).end()
      }
      _ <- sendRequest("DELETE", s"/files/${uploadResponse.getString("uuid")}")
    } yield request
  }

  @Test
  def deleteFolderRecursively(): Unit = okTest {
    val file = "/com/campudus/tableaux/uploads/Screen Shöt.jpg"
    val mimetype = "image/jpeg"

    implicit def convertStringToOptionalInt(i: String): Option[Int] = {
      Option(i.toInt)
    }

    def createFolderPutJson(parent: Option[Int] = None): JsonObject = {
      Json.obj("name" -> "Test Folder", "description" -> "Test Description", "parent" -> parent.orNull)
    }

    def createFilePutJson(folder: String): JsonObject = {
      Json.obj("name" -> "Test File", "description" -> "Test Description", "folder" -> folder)
    }

    for {
      folder1 <- sendRequestWithJson("POST", createFolderPutJson(), s"/folders").map(s => s.getString("id"))
      folder2 <- sendRequestWithJson("POST", createFolderPutJson(), s"/folders").map(s => s.getString("id"))

      folder11 <- sendRequestWithJson("POST", createFolderPutJson(folder1), s"/folders").map(s => s.getString("id"))
      folder12 <- sendRequestWithJson("POST", createFolderPutJson(folder1), s"/folders").map(s => s.getString("id"))

      folder21 <- sendRequestWithJson("POST", createFolderPutJson(folder2), s"/folders").map(s => s.getString("id"))
      folder22 <- sendRequestWithJson("POST", createFolderPutJson(folder2), s"/folders").map(s => s.getString("id"))

      file1 <- uploadFile(file, mimetype).map(f => f.getString("uuid"))
      puttedFile1 <- sendRequestWithJson("PUT", createFilePutJson(folder11), s"/files/$file1")

      file2 <- uploadFile(file, mimetype).map(f => f.getString("uuid"))
      puttedFile2 <- sendRequestWithJson("PUT", createFilePutJson(folder21), s"/files/$file2")

      deleteFolder1 <- sendRequest("DELETE", s"/folders/$folder1")
      deleteFolder2 <- sendRequest("DELETE", s"/folders/$folder2")
    } yield {
      assertEquals(folder1.toString, deleteFolder1.getString("id"))
      assertEquals(folder2.toString, deleteFolder2.getString("id"))
    }
  }

  @Test
  def createAttachmentColumn(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 3, "ordering" -> 3)))

    val column = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "attachment",
      "name" -> "Downloads"
    )))

    for {
      tableId <- setupDefaultTable()

      column <- sendRequestWithJson("POST", column, s"/tables/$tableId/columns")
    } yield {
      assertEquals(expectedJson, column)
    }
  }

  @Test
  def fillAndRetrieveAttachmentCell(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 3, "ordering" -> 3)))

    val column = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "attachment",
      "name" -> "Downloads"
    )))

    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"
    val putFile = Json.obj("name" -> "Test PDF", "description" -> "A description about that PDF.")

    for {
      tableId <- setupDefaultTable()

      columnId <- sendRequestWithJson("POST", column, s"/tables/$tableId/columns") map {
        json =>
          json.getArray("columns").get[JsonObject](0).getField[Int]("id")
      }

      rowId <- sendRequest("POST", s"/tables/$tableId/rows") map {
        json =>
          json.getArray("rows").get[JsonObject](0).getField[Int]("id")
      }

      fileUuid <- uploadFile(file, mimetype) map (_.getString("uuid"))
      _ <- sendRequestWithJson("PUT", putFile, s"/files/$fileUuid")

      // Add attachment
      resultFill <- sendRequestWithJson("POST", Json.obj("value" -> Json.obj("uuid" -> fileUuid)), s"/tables/$tableId/columns/$columnId/rows/$rowId")

      // Retrieve attachment
      resultRetrieve <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

      _ <- sendRequest("DELETE", s"/files/$fileUuid")
    } yield {
      assertEquals(3, columnId)

      assertEquals(Json.obj("status" -> "ok"), resultFill)

      assertEquals(fileUuid, resultRetrieve.getArray("rows").get[JsonObject](0).getArray("value").get[JsonObject](0).getString("uuid"))
    }
  }

  @Test
  def updateAttachmentColumn(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 3, "ordering" -> 3)))

    val column = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "attachment",
      "name" -> "Downloads"
    )))

    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"

    val putFile = Json.obj("name" -> "Test PDF", "description" -> "A description about that PDF.")

    for {
      tableId <- setupDefaultTable()

      columnId <- sendRequestWithJson("POST", column, s"/tables/$tableId/columns") map {
        json =>
          json.getArray("columns").get[JsonObject](0).getField[Int]("id")
      }

      rowId <- sendRequest("POST", s"/tables/$tableId/rows") map {
        json =>
          json.getArray("rows").get[JsonObject](0).getField[Int]("id")
      }

      fileUuid1 <- uploadFile(file, mimetype) map (_.getString("uuid"))
      _ <- sendRequestWithJson("PUT", putFile, s"/files/$fileUuid1")

      fileUuid2 <- uploadFile(file, mimetype) map (_.getString("uuid"))
      _ <- sendRequestWithJson("PUT", putFile, s"/files/$fileUuid2")

      // Add attachment
      resultFill1 <- sendRequestWithJson("POST", Json.obj("value" -> Json.obj("uuid" -> fileUuid1)), s"/tables/$tableId/columns/$columnId/rows/$rowId")
      resultFill2 <- sendRequestWithJson("POST", Json.obj("value" -> Json.obj("uuid" -> fileUuid2)), s"/tables/$tableId/columns/$columnId/rows/$rowId")

      // Retrieve attachment after fill
      resultRetrieveFill <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

      // Update attachment
      resultUpdate1 <- sendRequestWithJson("PUT", Json.obj("value" -> Json.obj("uuid" -> fileUuid1, "ordering" -> 2)), s"/tables/$tableId/columns/$columnId/rows/$rowId")
      resultUpdate2 <- sendRequestWithJson("PUT", Json.obj("value" -> Json.obj("uuid" -> fileUuid2, "ordering" -> 1)), s"/tables/$tableId/columns/$columnId/rows/$rowId")

      // Retrieve attachment after update
      resultRetrieveUpdate <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

      _ <- sendRequest("DELETE", s"/files/$fileUuid1")
      _ <- sendRequest("DELETE", s"/files/$fileUuid2")
    } yield {
      assertEquals(3, columnId)

      assertEquals(Json.obj("status" -> "ok"), resultFill1)
      assertEquals(Json.obj("status" -> "ok"), resultFill2)

      assertEquals(fileUuid1, resultRetrieveFill.getArray("rows").get[JsonObject](0).getArray("value").get[JsonObject](0).getString("uuid"))
      assertEquals(fileUuid2, resultRetrieveFill.getArray("rows").get[JsonObject](0).getArray("value").get[JsonObject](1).getString("uuid"))

      assertEquals(fileUuid2, resultRetrieveUpdate.getArray("rows").get[JsonObject](0).getArray("value").get[JsonObject](0).getString("uuid"))
      assertEquals(fileUuid1, resultRetrieveUpdate.getArray("rows").get[JsonObject](0).getArray("value").get[JsonObject](1).getString("uuid"))
    }
  }

  private def uploadFile(file: String, mimeType: String): Future[JsonObject] = {
    val filePath = getClass.getResource(file).toURI.getPath
    val fileName = file.substring(file.lastIndexOf("/") + 1)

    def requestHandler(req: HttpClientRequest): Unit = {
      val boundary = "dLV9Wyq26L_-JQxk6ferf-RT153LhOO"
      val header =
        "--" + boundary + "\r\n" +
          "Content-Disposition: form-data; name=\"file\"; filename=\"" + fileName + "\"\r\n" +
          "Content-Type: " + mimeType + "\r\n\r\n"
      val footer = "\r\n--" + boundary + "--\r\n"

      val contentLength = String.valueOf(vertx.fileSystem.propsSync(filePath).size() + header.length + footer.length)
      req.putHeader("Content-length", contentLength)
      req.putHeader("Content-type", s"multipart/form-data; boundary=$boundary")

      logger.info(s"Loading file '$filePath' from disc, content-length=$contentLength")
      req.write(header)
      vertx.fileSystem.open(filePath, { ar =>
        assertTrue(s"Should be able to open file $filePath", ar.succeeded())
        val file = ar.result()
        val pump = Pump.createPump(file, req)
        file.endHandler({
          file.close({ ar =>
            if (ar.succeeded()) {
              logger.info(s"File loaded, ending request, ${pump.bytesPumped()} bytes pumped.")
              req.end(footer)
            } else {
              fail(ar.cause().getMessage)
            }
          })
        })

        pump.start()
      })
    }

    import FutureUtils._

    promisify { p: Promise[JsonObject] =>
      requestHandler(httpRequest("POST", "/files", jsonResponse(p)))
    }
  }
}
