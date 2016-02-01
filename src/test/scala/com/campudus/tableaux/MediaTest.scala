package com.campudus.tableaux

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.{File, Folder, MultiLanguageValue}
import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.database.model.{FileModel, FolderModel}
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpClientResponse
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.FunctionConverters._
import io.vertx.scala.FutureHelper._
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonArray}

import scala.concurrent.{Future, Promise}
import scala.reflect.io.Path
import scala.util.{Failure, Random, Success, Try}

@RunWith(classOf[VertxUnitRunner])
class MediaTest extends TableauxTestBase {

  def createFileModel(): FileModel = {
    val sqlConnection = SQLConnection(verticle, databaseConfig)
    val dbConnection = DatabaseConnection(verticle, sqlConnection)

    new FileModel(dbConnection)
  }

  def createFolderModel(): FolderModel = {
    val sqlConnection = SQLConnection(verticle, databaseConfig)
    val dbConnection = DatabaseConnection(verticle, sqlConnection)

    new FolderModel(dbConnection)
  }

  @Test
  def testFileModel(implicit c: TestContext): Unit = okTest {
    def file = File(
      None,
      None,
      MultiLanguageValue("de_DE" -> "Test"),
      MultiLanguageValue.empty(),
      MultiLanguageValue("de_DE" -> s"internal${Random.nextInt()}.pdf"),
      MultiLanguageValue("de_DE" -> "external.pdf"),
      MultiLanguageValue("de_DE" -> "application/pdf"),
      None,
      None
    )

    for {
      model <- Future.successful(createFileModel())

      tempFile1 <- model.add(file)
      tempFile2 <- model.add(file)

      sizeAfterAdd <- model.size()

      insertedFile1 <- model.update(tempFile1)
      insertedFile2 <- model.update(tempFile2)

      retrievedFile <- model.retrieve(insertedFile1.uuid.get)
      updatedFile <- model.update(File(uuid = retrievedFile.uuid.get, MultiLanguageValue("de_DE" -> "Changed"), MultiLanguageValue("de_DE" -> "Changed"), MultiLanguageValue("de_DE" -> "external.pdf"), None))

      allFiles <- model.retrieveAll()

      size <- model.size()

      _ <- model.delete(insertedFile1)

      sizeAfterDeleteOne <- model.size()
    } yield {
      assertEquals(0L, sizeAfterAdd)

      assertEquals(insertedFile1, retrievedFile)

      assert(retrievedFile.updatedAt.isDefined)

      assert(updatedFile.createdAt.isDefined)
      assert(updatedFile.updatedAt.isDefined)

      assert(updatedFile.updatedAt.get.isAfter(updatedFile.createdAt.get))

      assertEquals(2, allFiles.toList.size)

      assertEquals(2L, size)
      assertEquals(1L, sizeAfterDeleteOne)
    }
  }

  @Test
  def testFolderModel(implicit c: TestContext): Unit = okTest {
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

      assertEquals(2L, size)
      assertEquals(1L, sizeAfterDelete)
    }
  }

  @Test
  def retrieveRootFolder(implicit c: TestContext): Unit = okTest {
    for {
      rootFolder <- sendRequest("GET", "/folders?langtag=de-DE")
    } yield {
      assertNull(rootFolder.getString("id"))
      assertEquals("root", rootFolder.getString("name"))
      assertNull(rootFolder.getString("parent"))
    }
  }

  @Test
  def createAndRetrieveFolder(implicit c: TestContext): Unit = okTest {
    def createFolderPutJson(name: String): JsonObject = {
      Json.obj("name" -> name, "description" -> "Test Description", "parent" -> null)
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

  @Test
  def uploadFileWithNonAsciiCharacterName(implicit c: TestContext): Unit = okTest {
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

      assertEquals(meta.getObject("title"), uploadedFile.getObject("title"))
      assertEquals(Path(filePath).name, uploadedFile.getObject("externalName").getString("zh_CN"))
      assertEquals(false, uploadedFile.containsField("tmp"))

      assertEquals(false, puttedFile.containsField("tmp"))
      assertEquals(meta.getObject("name"), puttedFile.getObject("name"))
      assertEquals(meta.getObject("description"), puttedFile.getObject("description"))

      assertEquals(uploadedFile.getObject("externalName"), puttedFile.getObject("externalName"))

      // We need to remove url because DELETE doesn't return ExtendedFile
      puttedFile.removeField("url")
      assertEquals(puttedFile, deletedFile)
    }
  }

  @Test
  def retrieveFile(implicit c: TestContext): Unit = okTest {
    val fileName = "Scr$en Shot.pdf"
    val filePath = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"
    val size = vertx.fileSystem.propsBlocking(getClass.getResource(filePath).toURI.getPath).size()

    val meta = Json.obj(
      "title" -> Json.obj(
        "de_DE" -> "Test PDF"
      ),
      "description" -> Json.obj(
        "de_DE" -> "A description about that PDF."
      )
    )

    for {
      file <- sendRequest("POST", "/files", meta)

      uploadedFile <- replaceFile(file.getString("uuid"), "de_DE", filePath, mimetype)
      puttedFile <- sendRequest("PUT", s"/files/${file.getString("uuid")}", meta)

      file <- sendRequest("GET", s"/files/${file.getString("uuid")}")

      _ <- futurify { p: Promise[Unit] =>
        val url = file.getObject("url").getString("de_DE")

        httpRequest("GET", s"$url", {
          resp: HttpClientResponse =>
            assertEquals(200, resp.statusCode())

            assertEquals("Should get the correct MIME type", mimetype, resp.getHeader("content-type"))
            assertEquals("Should get the correct content length", s"$size", resp.getHeader("content-length"))

            resp.bodyHandler { buf: Buffer =>
              assertEquals("Should get the same size back as the file really is", size, buf.length())
              p.success(())
            }
        }, { x =>
          c.fail(x)
          p.failure(x)
        }).end()
      }

      _ <- sendRequest("DELETE", s"/files/${file.getString("uuid")}")
    } yield {
      assertEquals(meta.getObject("title"), file.getObject("title"))
      assertEquals(meta.getObject("description"), file.getObject("description"))
    }
  }

  @Test
  def retrieveFileWithMultiLanguage(implicit c: TestContext): Unit = okTest {
    val fileName = "Scr$en Shot.pdf"
    val filePath = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"

    val putOne = Json.obj(
      "title" -> Json.obj(
        "de_DE" -> "Ein schöner deutscher Titel."
      ),
      "description" -> Json.obj(
        "de_DE" -> "Und hier folgt eine tolle hochdeutsche Beschreibung."
      )
    )

    val putTwo = Json.obj(
      "title" -> Json.obj(
        "en_GB" -> "A beautiful German title."
      ),
      "description" -> Json.obj(
        "en_GB" -> "And here is a great High German description."
      )
    )

    for {
      file <- sendRequest("POST", "/files", putOne)

      uploadedFile <- replaceFile(file.getString("uuid"), "de_DE", filePath, mimetype)

      _ <- sendRequest("PUT", s"/files/${file.getString("uuid")}", putOne)
      _ <- sendRequest("PUT", s"/files/${file.getString("uuid")}", putTwo)

      fileRequested <- sendRequest("GET", s"/files/${file.getString("uuid")}")

      _ <- replaceFile(file.getString("uuid"), "en_GB", filePath, mimetype)
      fileReplaced <- sendRequest("GET", s"/files/${file.getString("uuid")}")

      _ <- replaceFile(file.getString("uuid"), "de_DE", filePath, mimetype)
      fileReplaced2 <- sendRequest("GET", s"/files/${file.getString("uuid")}")

      deletedEnGB <- sendRequest("DELETE", s"/files/${file.getString("uuid")}/en_GB")

      _ <- sendRequest("DELETE", s"/files/${file.getString("uuid")}")
    } yield {
      assertEquals(putOne.getObject("title").mergeIn(putTwo.getObject("title")), fileRequested.getObject("title"))

      assertTrue(!file.getObject("url").containsField("en_GB"))
      assertTrue(fileReplaced.getObject("url").containsField("en_GB") && fileReplaced.getObject("url").getString("en_GB") != null)

      assertNotSame(fileReplaced.getObject("internalName").getString("de_DE"), fileReplaced2.getObject("internalName").getString("de_DE"))

      assertTrue(!deletedEnGB.getObject("internalName").containsField("en_GB"))
    }
  }

  @Test
  def deleteFolderRecursively(implicit c: TestContext): Unit = okTest {
    val filePath = "/com/campudus/tableaux/uploads/Screen Shöt.jpg"
    val mimetype = "image/jpeg"

    def createFolderPutJson(parent: Option[Int] = None): JsonObject = {
      Json.obj("name" -> "Test Folder", "description" -> "Test Description", "parent" -> parent.orNull)
    }

    def createFilePutJson(folder: Int): JsonObject = {
      Json.obj("title" -> Json.obj("de_DE" -> "Test File"), "description" -> Json.obj("de_DE" -> "Test Description"), "folder" -> folder)
    }

    for {
      folder1 <- sendRequest("POST", s"/folders", createFolderPutJson()).map(_.getInteger("id"))
      folder2 <- sendRequest("POST", s"/folders", createFolderPutJson()).map(_.getInteger("id"))

      folder11 <- sendRequest("POST", s"/folders", createFolderPutJson(Some(folder1))).map(_.getInteger("id"))
      folder12 <- sendRequest("POST", s"/folders", createFolderPutJson(Some(folder1))).map(_.getInteger("id"))

      folder21 <- sendRequest("POST", s"/folders", createFolderPutJson(Some(folder2))).map(_.getInteger("id"))
      folder22 <- sendRequest("POST", s"/folders", createFolderPutJson(Some(folder2))).map(_.getInteger("id"))

      file1 <- sendRequest("POST", "/files", createFilePutJson(folder11)).map(f => f.getString("uuid"))
      _ <- replaceFile(file1, "de_DE", filePath, mimetype)
      puttedFile1 <- sendRequest("PUT", s"/files/$file1", createFilePutJson(folder11))

      file2 <- sendRequest("POST", "/files", createFilePutJson(folder21)).map(f => f.getString("uuid"))
      _ <- replaceFile(file2, "de_DE", filePath, mimetype)
      puttedFile2 <- sendRequest("PUT", s"/files/$file2", createFilePutJson(folder21))

      deleteFolder1 <- sendRequest("DELETE", s"/folders/$folder1")
      deleteFolder2 <- sendRequest("DELETE", s"/folders/$folder2")
    } yield {
      assertEquals(folder1, deleteFolder1.getInteger("id"))
      assertEquals(folder2, deleteFolder2.getInteger("id"))
    }
  }

  @Test
  def deleteTmpFile(implicit c: TestContext): Unit = okTest {
    val filePath = "/com/campudus/tableaux/uploads/Screen Shöt.jpg"
    val mimetype = "image/jpeg"

    val meta = Json.obj(
      "title" -> Json.obj(
        "en_GB" -> "A beautiful German title."
      ),
      "description" -> Json.obj(
        "en_GB" -> "And here is a great High German description."
      )
    )

    for {
      tmpFile <- sendRequest("POST", "/files", meta)

      uploadedFile <- replaceFile(tmpFile.getString("uuid"), "de_DE", filePath, mimetype)
      deletedFile <- sendRequest("DELETE", s"/files/${tmpFile.getString("uuid")}")
    } yield {
      assertEquals(tmpFile.getString("uuid"), uploadedFile.getString("uuid"))
      assertEquals(tmpFile.getString("uuid"), deletedFile.getString("uuid"))
    }
  }

  @Test
  def deleteAlreadyDeletedTmpFile(implicit c: TestContext): Unit = okTest {
    val file = "/com/campudus/tableaux/uploads/Screen Shöt.jpg"
    val mimetype = "image/jpeg"

    val meta = Json.obj(
      "title" -> Json.obj(
        "en_GB" -> "A beautiful German title."
      ),
      "description" -> Json.obj(
        "en_GB" -> "And here is a great High German description."
      )
    )

    for {
      tmpFile <- sendRequest("POST", "/files", meta)
      uploadedFile <- replaceFile(tmpFile.getString("uuid"), "de_DE", file, mimetype)

      _ <- {
        val uploadsDirectory = Path(s"${tableauxConfig.workingDirectory}/${tableauxConfig.uploadsDirectory}")

        val path = uploadsDirectory / Path(uploadedFile.getObject("internalName").getString("de_DE"))

        futurify({ p: Promise[Unit] =>
          // delete tmp file
          vertx.fileSystem().delete(path.toString(), {
            case Success(_) =>
              p.success(())
            case Failure(e) =>
              p.failure(e)
          }: Try[Void] => Unit)
        })
      }

      result <- sendRequest("DELETE", s"/files/${tmpFile.getString("uuid")}")
    } yield {
      assertEquals(tmpFile.getString("uuid"), result.getString("uuid"))
    }
  }

  @Test
  def createAttachmentColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 3, "ordering" -> 3)))

    val column = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "attachment",
      "name" -> "Downloads"
    )))

    for {
      tableId <- setupDefaultTable()

      column <- sendRequest("POST", s"/tables/$tableId/columns", column)
    } yield {
      assertEquals(expectedJson, column)
    }
  }

  @Test
  def fillAndRetrieveAttachmentCell(implicit c: TestContext): Unit = okTest {
    val column = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "attachment",
      "name" -> "Downloads"
    )))

    val fileName = "Scr$en Shot.pdf"
    val filePath = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"
    val putFile = Json.obj("title" -> Json.obj("de_DE" -> "Test PDF"), "description" -> Json.obj("de_DE" -> "A description about that PDF."))

    for {
      tableId <- setupDefaultTable()

      columnId <- sendRequest("POST", s"/tables/$tableId/columns", column).map(_.getArray("columns").get[JsonObject](0).getInteger("id"))

      rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))

      fileUuid <- createFile("de_DE", filePath, mimetype, None) map (_.getString("uuid"))
      _ <- sendRequest("PUT", s"/files/$fileUuid", putFile)

      // Add attachment
      resultFill <- sendRequest("POST", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Json.obj("uuid" -> fileUuid)))

      // Retrieve attachment
      resultRetrieve <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

      _ <- sendRequest("DELETE", s"/files/$fileUuid")
    } yield {
      assertEquals(3, columnId)

      assertEquals(Json.obj("status" -> "ok"), resultFill)

      assertEquals(fileUuid, resultRetrieve.getArray("value").get[JsonObject](0).getString("uuid"))
    }
  }

  @Test
  def fillAndReplaceAndRetrieveAttachmentCell(implicit c: TestContext): Unit = okTest {
    val column = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "attachment",
      "name" -> "Downloads"
    )))

    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"
    val putFile = Json.obj("title" -> Json.obj("de_DE" -> "Test PDF"), "description" -> Json.obj("de_DE" -> "A description about that PDF."))

    for {
      tableId <- setupDefaultTable()

      columnId <- sendRequest("POST", s"/tables/$tableId/columns", column).map(_.getArray("columns").get[JsonObject](0).getInteger("id"))

      rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))

      fileUuid1 <- createFile("de_DE", file, mimetype, None) map (_.getString("uuid"))
      _ <- sendRequest("PUT", s"/files/$fileUuid1", putFile)
      fileUuid2 <- createFile("de_DE", file, mimetype, None) map (_.getString("uuid"))
      _ <- sendRequest("PUT", s"/files/$fileUuid2", putFile)
      fileUuid3 <- createFile("de_DE", file, mimetype, None) map (_.getString("uuid"))
      _ <- sendRequest("PUT", s"/files/$fileUuid3", putFile)

      // Add attachment
      resultFill <- sendRequest("POST", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Json.obj("uuid" -> fileUuid1)))

      // Retrieve row with attachment
      resultRetrieve <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

      // Replace with attachments (with order)
      resultReplace <- sendRequest("POST", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Json.arr(Json.obj("uuid" -> fileUuid2, "ordering" -> 2), Json.obj("uuid" -> fileUuid3, "ordering" -> 1))))

      // Retrieve attachments after replace
      resultRetrieveAfterReplace <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

      // Replace with attachments (without order)
      resultReplaceWithoutOrder <- sendRequest("POST", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Json.arr(Json.obj("uuid" -> fileUuid2), Json.obj("uuid" -> fileUuid3))))

      // Retrieve attachments after replace
      resultRetrieveAfterReplaceWithoutOrder <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

      // Replace with no attachments
      resultReplaceEmpty <- sendRequest("POST", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Json.arr()))

      // Retrieve attachments after replace
      resultRetrieveAfterReplaceEmpty <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

      _ <- sendRequest("DELETE", s"/files/$fileUuid1")
      _ <- sendRequest("DELETE", s"/files/$fileUuid2")
      _ <- sendRequest("DELETE", s"/files/$fileUuid3")
    } yield {
      assertEquals(3, columnId)

      assertEquals(Json.obj("status" -> "ok"), resultFill)

      assertEquals(fileUuid1, resultRetrieve.getArray("values").get[JsonArray](columnId - 1).get[JsonObject](0).getString("uuid"))

      assertEquals(Json.obj("status" -> "ok"), resultReplace)

      assertEquals(fileUuid3, resultRetrieveAfterReplace.getArray("value").get[JsonObject](0).getString("uuid"))
      assertEquals(fileUuid2, resultRetrieveAfterReplace.getArray("value").get[JsonObject](1).getString("uuid"))

      assertEquals(Json.obj("status" -> "ok"), resultReplaceWithoutOrder)

      assertEquals(fileUuid2, resultRetrieveAfterReplaceWithoutOrder.getArray("value").get[JsonObject](0).getString("uuid"))
      assertEquals(fileUuid3, resultRetrieveAfterReplaceWithoutOrder.getArray("value").get[JsonObject](1).getString("uuid"))

      assertEquals(Json.obj("status" -> "ok"), resultReplaceEmpty)

      assertEquals(0, resultRetrieveAfterReplaceEmpty.getArray("value").size())
    }
  }

  @Test
  def addAttachmentWithMalformedUUID(implicit c: TestContext): Unit = exceptionTest("error.arguments") {
    val column = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "attachment",
      "name" -> "Downloads"
    )))

    for {
      tableId <- setupDefaultTable()
      columnId <- sendRequest("POST", s"/tables/$tableId/columns", column).map(_.getArray("columns").get[JsonObject](0).getInteger("id"))
      rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))

      // Add attachment with malformed uuid
      resultFill <- sendRequest("POST", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Json.obj("uuid" -> "this-is-not-an-uuid")))
    } yield {
      resultFill
    }
  }

  @Test
  def updateAttachmentColumn(implicit c: TestContext): Unit = okTest {
    val column = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "attachment",
      "name" -> "Downloads"
    )))

    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"

    val putFile = Json.obj("title" -> Json.obj("de_DE" -> "Test PDF"), "description" -> Json.obj("de_DE" -> "A description about that PDF."))

    for {
      tableId <- setupDefaultTable()

      columnId <- sendRequest("POST", s"/tables/$tableId/columns", column).map(_.getArray("columns").get[JsonObject](0).getInteger("id"))

      rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getInteger("id"))

      fileUuid1 <- createFile("de_DE", file, mimetype, None) map (_.getString("uuid"))
      _ <- sendRequest("PUT", s"/files/$fileUuid1", putFile)

      fileUuid2 <- createFile("de_DE", file, mimetype, None) map (_.getString("uuid"))
      _ <- sendRequest("PUT", s"/files/$fileUuid2", putFile)

      // Add attachments
      resultFill1 <- sendRequest("POST", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Json.obj("uuid" -> fileUuid1, "ordering" -> 1)))
      resultFill2 <- sendRequest("POST", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Json.obj("uuid" -> fileUuid2)))

      // Retrieve attachments after fill
      resultRetrieveFill <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

      // Update attachments
      resultUpdate1 <- sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Json.obj("uuid" -> fileUuid1, "ordering" -> 2)))
      resultUpdate2 <- sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Json.obj("uuid" -> fileUuid2, "ordering" -> 1)))

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

      assertEquals(Json.obj("status" -> "ok"), resultFill1)
      assertEquals(Json.obj("status" -> "ok"), resultFill2)

      assertEquals(fileUuid1, resultRetrieveFill.getArray("value").get[JsonObject](0).getString("uuid"))
      assertEquals(fileUuid2, resultRetrieveFill.getArray("value").get[JsonObject](1).getString("uuid"))

      assertEquals(fileUuid2, resultRetrieveUpdate.getArray("value").get[JsonObject](0).getString("uuid"))
      assertEquals(fileUuid1, resultRetrieveUpdate.getArray("value").get[JsonObject](1).getString("uuid"))

      assertEquals(fileUuid1, resultRetrieveDelete.getArray("value").get[JsonObject](0).getString("uuid"))
    }
  }

  @Test
  def mergeFiles(implicit c: TestContext): Unit = okTest {
    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"

    val putFile1 = Json.obj("title" -> Json.obj("de_DE" -> "Test PDF 1"), "description" -> Json.obj("de_DE" -> "A description about that PDF. 1"))
    val putFile2 = Json.obj("title" -> Json.obj("en_US" -> "Test PDF 2"), "description" -> Json.obj("en_US" -> "A description about that PDF. 2"))

    for {
      fileUuid1 <- createFile("de_DE", file, mimetype, None) map (_.getString("uuid"))
      fileAfterPut1 <- sendRequest("PUT", s"/files/$fileUuid1", putFile1)

      fileUuid2 <- createFile("en_US", file, mimetype, None) map (_.getString("uuid"))
      fileAfterPut2 <- sendRequest("PUT", s"/files/$fileUuid2", putFile2)

      _ <- sendRequest("POST", s"/files/$fileUuid1/merge", Json.obj("mergeWith" -> fileUuid2, "langtag" -> "en_US"))

      fileAfterMerge <- sendRequest("GET", s"/files/$fileUuid1")

      files <- sendRequest("GET", s"/folders?langtag=de_DE")

      _ <- sendRequest("DELETE", s"/files/$fileUuid1")
    } yield {
      assertEquals(1, files.getJsonArray("files", Json.emptyArr()).size())

      assertEquals(fileAfterPut1.getJsonObject("internalName").getString("de_DE"), fileAfterMerge.getJsonObject("internalName").getString("de_DE"))
      assertEquals(fileAfterPut2.getJsonObject("internalName").getString("en_US"), fileAfterMerge.getJsonObject("internalName").getString("en_US"))
    }
  }

  @Test
  def testFileSorting(implicit c: TestContext): Unit = okTest {
    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"

    val putFileA = Json.obj("externalName" -> Json.obj("de_DE" -> "A.pdf"))
    val putFileB = Json.obj("externalName" -> Json.obj("de_DE" -> "B.pdf"))
    val putFileC = Json.obj("externalName" -> Json.obj("de_DE" -> "C.pdf"))

    for {
      fileUuid1 <- createFile("de_DE", file, mimetype, None) map (_.getString("uuid"))
      fileAfterPut1 <- sendRequest("PUT", s"/files/$fileUuid1", putFileC)

      fileUuid2 <- createFile("de_DE", file, mimetype, None) map (_.getString("uuid"))
      fileAfterPut2 <- sendRequest("PUT", s"/files/$fileUuid2", putFileA)

      fileUuid3 <- createFile("de_DE", file, mimetype, None) map (_.getString("uuid"))
      fileAfterPut3 <- sendRequest("PUT", s"/files/$fileUuid3", putFileB)

      files <- sendRequest("GET", s"/folders?langtag=de_DE")

      _ <- sendRequest("DELETE", s"/files/$fileUuid1")
      _ <- sendRequest("DELETE", s"/files/$fileUuid2")
      _ <- sendRequest("DELETE", s"/files/$fileUuid3")
    } yield {
      assertEquals(3, files.getJsonArray("files", Json.emptyArr()).size())

      assertEquals("A.pdf", files.getJsonArray("files", Json.emptyArr()).getJsonObject(0).getJsonObject("externalName").getString("de_DE"))
      assertEquals("B.pdf", files.getJsonArray("files", Json.emptyArr()).getJsonObject(1).getJsonObject("externalName").getString("de_DE"))
      assertEquals("C.pdf", files.getJsonArray("files", Json.emptyArr()).getJsonObject(2).getJsonObject("externalName").getString("de_DE"))

    }
  }

  private def createFile(langtag: String, filePath: String, mimeType: String, folder: Option[FolderId])(implicit c: TestContext): Future[JsonObject] = {
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

  private def replaceFile(uuid: String, langtag: String, file: String, mimeType: String)(implicit c: TestContext): Future[JsonObject] = {
    uploadFile("PUT", s"/files/$uuid/$langtag", file, mimeType)
  }


  // Change file meta information
  @Test
  def testChangeFileMetaInformation(implicit c: TestContext): Unit = okTest {
    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"

    for {
      fileAfterCreate <- createFile("de_DE", file, mimetype, None)
      (fileUuid, internalNameDe) <- Future.successful((fileAfterCreate.getString("uuid"), fileAfterCreate.getObject("internalName").getString("de_DE")))
      fileAfterUploadEn <- uploadFile("PUT", s"/files/$fileUuid/en_GB", file, mimetype)
      internalNameEn <- Future.successful(fileAfterUploadEn.getObject("internalName").getString("en_GB"))
      fileAfterChange <- sendRequest("PUT", s"/files/$fileUuid", Json.obj(
        "title" -> Json.obj(
          "de_DE" -> "A_de.pdf",
          "en_GB" -> "A_en.pdf"
        ),
        "description" -> Json.obj(
          "de_DE" -> "desc deutsch",
          "en_GB" -> "desc english"
        ),
        "externalName" -> Json.obj(
          "de_DE" -> "A_de.pdf",
          "en_GB" -> "A_en.pdf"
        ),
        "internalName" -> Json.obj(
          "de_DE" -> internalNameEn,
          "en_GB" -> internalNameDe
        ),
        "mimeType" -> Json.obj(
          "de_DE" -> "application/pdf",
          "en_GB" -> "application/pdf"
        )
      ))

      _ <- sendRequest("DELETE", s"/files/$fileUuid")
    } yield {
      assertEquals("A_de.pdf", fileAfterChange.getJsonObject("title").getString("de_DE"))
      assertEquals("desc deutsch", fileAfterChange.getJsonObject("description").getString("de_DE"))
      assertEquals("A_de.pdf", fileAfterChange.getJsonObject("externalName").getString("de_DE"))
      assertEquals(internalNameEn, fileAfterChange.getJsonObject("internalName").getString("de_DE"))
      assertEquals("application/pdf", fileAfterChange.getJsonObject("mimeType").getString("de_DE"))

      assertEquals("A_en.pdf", fileAfterChange.getJsonObject("title").getString("en_GB"))
      assertEquals("desc english", fileAfterChange.getJsonObject("description").getString("en_GB"))
      assertEquals("A_en.pdf", fileAfterChange.getJsonObject("externalName").getString("en_GB"))
      assertEquals(internalNameDe, fileAfterChange.getJsonObject("internalName").getString("en_GB"))
      assertEquals("application/pdf", fileAfterChange.getJsonObject("mimeType").getString("en_GB"))

    }
  }

  @Test
  def testFailChangeFileMetaInformationWithPathInFile1(implicit c: TestContext): Unit = exceptionTest("error.request.invalid") {
    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"

    for {
      fileAfterCreate <- createFile("de_DE", file, mimetype, None)
      (fileUuid, internalNameDe) <- Future.successful((fileAfterCreate.getString("uuid"), fileAfterCreate.getObject("internalName").getString("de_DE")))
      exception <- sendRequest("PUT", s"/files/$fileUuid", Json.obj(
        "title" -> Json.obj(
          "de_DE" -> "A_de.pdf"
        ),
        "description" -> Json.obj(
          "de_DE" -> "desc deutsch"
        ),
        "externalName" -> Json.obj(
          "de_DE" -> "A_de.pdf"
        ),
        "internalName" -> Json.obj(
          "de_DE" -> "../blablubb.config"
        ),
        "mimeType" -> Json.obj(
          "de_DE" -> "application/pdf"
        )
      )) recover {
        case ex => ex
      }

      _ <- sendRequest("DELETE", s"/files/$fileUuid")
    } yield {
      exception match {
        case ex: Throwable => throw ex
      }
    }
  }

  @Test
  def testFailChangeFileMetaInformationWithPathInFile2(implicit c: TestContext): Unit = exceptionTest("error.request.invalid") {
    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"

    for {
      fileAfterCreate <- createFile("de_DE", file, mimetype, None)
      (fileUuid, internalNameDe) <- Future.successful((fileAfterCreate.getString("uuid"), fileAfterCreate.getObject("internalName").getString("de_DE")))
      exception <- sendRequest("PUT", s"/files/$fileUuid", Json.obj(
        "title" -> Json.obj(
          "de_DE" -> "A_de.pdf"
        ),
        "description" -> Json.obj(
          "de_DE" -> "desc deutsch"
        ),
        "externalName" -> Json.obj(
          "de_DE" -> "A_de.pdf"
        ),
        "internalName" -> Json.obj(
          "de_DE" -> "..\\bla\\blablubb.config"
        ),
        "mimeType" -> Json.obj(
          "de_DE" -> "application/pdf"
        )
      )) recover {
        case ex => ex
      }

      _ <- sendRequest("DELETE", s"/files/$fileUuid")
    } yield {
      exception match {
        case ex: Throwable => throw ex
      }
    }
  }

  @Test
  def testFailChangeFileMetaInformationWithInvalidFile1(implicit c: TestContext): Unit = exceptionTest("error.request.invalid") {
    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"

    for {
      fileAfterCreate <- createFile("de_DE", file, mimetype, None)
      (fileUuid, internalNameDe) <- Future.successful((fileAfterCreate.getString("uuid"), fileAfterCreate.getObject("internalName").getString("de_DE")))
      exception <- sendRequest("PUT", s"/files/$fileUuid", Json.obj(
        "title" -> Json.obj(
          "de_DE" -> "A_de.pdf"
        ),
        "description" -> Json.obj(
          "de_DE" -> "desc deutsch"
        ),
        "externalName" -> Json.obj(
          "de_DE" -> "A_de.pdf"
        ),
        "internalName" -> Json.obj(
          "de_DE" -> ".."
        ),
        "mimeType" -> Json.obj(
          "de_DE" -> "application/pdf"
        )
      )) recover {
        case ex => ex
      }

      _ <- sendRequest("DELETE", s"/files/$fileUuid")
    } yield {
      exception match {
        case ex: Throwable => throw ex
      }
    }
  }

  @Test
  def testFailChangeFileMetaInformationWithInvalidFile2(implicit c: TestContext): Unit = exceptionTest("error.request.invalid") {
    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"

    for {
      fileAfterCreate <- createFile("de_DE", file, mimetype, None)
      (fileUuid, internalNameDe) <- Future.successful((fileAfterCreate.getString("uuid"), fileAfterCreate.getObject("internalName").getString("de_DE")))
      exception <- sendRequest("PUT", s"/files/$fileUuid", Json.obj(
        "title" -> Json.obj(
          "de_DE" -> "A_de.pdf"
        ),
        "description" -> Json.obj(
          "de_DE" -> "desc deutsch"
        ),
        "externalName" -> Json.obj(
          "de_DE" -> "A_de.pdf"
        ),
        "internalName" -> Json.obj(
          "de_DE" -> "."
        ),
        "mimeType" -> Json.obj(
          "de_DE" -> "application/pdf"
        )
      )) recover {
        case ex => ex
      }

      _ <- sendRequest("DELETE", s"/files/$fileUuid")
    } yield {
      exception match {
        case ex: Throwable => throw ex
      }
    }
  }

  @Test
  def testChangeFileInternalnameToNull(implicit c: TestContext): Unit = okTest {
    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"

    for {
      fileAfterCreate <- createFile("de_DE", file, mimetype, None)
      (fileUuid, internalNameDe) <- Future.successful((fileAfterCreate.getString("uuid"), fileAfterCreate.getObject("internalName").getString("de_DE")))
      fileAfterChange <- sendRequest("PUT", s"/files/$fileUuid", Json.obj(
        "title" -> Json.obj(
          "de_DE" -> "A_de.pdf"
        ),
        "description" -> Json.obj(
          "de_DE" -> "desc deutsch"
        ),
        "externalName" -> Json.obj(
          "de_DE" -> "A_de.pdf"
        ),
        "internalName" -> Json.obj(
          "de_DE" -> null
        ),
        "mimeType" -> Json.obj(
          "de_DE" -> null
        )
      ))

      _ <- sendRequest("DELETE", s"/files/$fileUuid")
    } yield {
      assertEquals("A_de.pdf", fileAfterChange.getJsonObject("title").getString("de_DE"))
      assertEquals("desc deutsch", fileAfterChange.getJsonObject("description").getString("de_DE"))
      assertEquals("A_de.pdf", fileAfterChange.getJsonObject("externalName").getString("de_DE"))
      assertEquals(null, fileAfterChange.getJsonObject("internalName").getString("de_DE"))
      assertEquals(null, fileAfterChange.getJsonObject("mimeType").getString("de_DE"))
    }
  }

  @Test
  def testChangeFileInternalnameAndMimeType(implicit c: TestContext): Unit = okTest {
    val fileName = "Scr$en Shot.pdf"
    val file = s"/com/campudus/tableaux/uploads/$fileName"
    val mimetype = "application/pdf"

    for {
      fileAfterCreate <- createFile("de_DE", file, mimetype, None)
      (fileUuid, internalNameDe) <- Future.successful((fileAfterCreate.getString("uuid"), fileAfterCreate.getObject("internalName").getString("de_DE")))
      fileAfterChange <- sendRequest("PUT", s"/files/$fileUuid", Json.obj(
        "title" -> Json.obj(
          "de_DE" -> "A_de.pdf"
        ),
        "description" -> Json.obj(
          "de_DE" -> "desc deutsch"
        ),
        "externalName" -> Json.obj(
          "de_DE" -> "A_de.pdf"
        ),
        "internalName" -> Json.obj(
          "de_DE" -> internalNameDe
        ),
        "mimeType" -> Json.obj(
          "de_DE" -> "text/plain"
        )
      ))

      _ <- sendRequest("DELETE", s"/files/$fileUuid")
    } yield {
      assertEquals("A_de.pdf", fileAfterChange.getJsonObject("title").getString("de_DE"))
      assertEquals("desc deutsch", fileAfterChange.getJsonObject("description").getString("de_DE"))
      assertEquals("A_de.pdf", fileAfterChange.getJsonObject("externalName").getString("de_DE"))
      assertEquals(internalNameDe, fileAfterChange.getJsonObject("internalName").getString("de_DE"))
      assertEquals("text/plain", fileAfterChange.getJsonObject("mimeType").getString("de_DE"))
    }
  }
}
