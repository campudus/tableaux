package com.campudus.tableaux.api.auth

import com.campudus.tableaux.UnauthorizedException
import com.campudus.tableaux.api.media.MediaTestBase
import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.{DomainObject, MultiLanguageValue}
import com.campudus.tableaux.database.model.{AttachmentModel, FileModel, FolderModel}
import com.campudus.tableaux.router.auth.permission.{DeleteMedia, EditMedia, RoleModel, TableauxUser}

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.core.json.{Json, JsonObject}

import java.util.UUID
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.JSONCompareMode

trait MediaControllerAuthTestBase extends MediaTestBase {

  def createMediaController(roleModel: RoleModel = RoleModel(Json.emptyObj())): MediaController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val attachmentModel = AttachmentModel(dbConnection)

    MediaController(tableauxConfig, createFolderModel, createFileModel, attachmentModel, roleModel)
  }

  def createFileModel: FileModel = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

    FileModel(dbConnection)
  }

  def createFolderModel: FolderModel = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

    FolderModel(dbConnection)
  }

  def insertTestFile() = {
    val fileModel = createFileModel
    fileModel
      .add(MultiLanguageValue("de" -> "file"), MultiLanguageValue.empty(), MultiLanguageValue("de" -> "file.pdf"), None)
  }

  def insertTestFolder() = {
    val folderJson = Json.obj("name" -> "TestFolder", "description" -> "Test Description", "parent" -> null)
    sendRequest("POST", s"/folders", folderJson).map(_.getLong("id"))
  }

  def getPermission(domainObject: DomainObject): JsonObject = {
    domainObject.getJson.getJsonObject("permission")
  }
}

@RunWith(classOf[VertxUnitRunner])
class MediaControllerAuthTest_checkAuthorization extends MediaControllerAuthTestBase {

  @Test
  def createFolder_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "create-media": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["createMedia"]
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createMediaController(roleModel)
    controller.addNewFolder("folder", "", None)
  }

  @Test
  def createFolder_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {

      val controller = createMediaController()
      controller.addNewFolder("folder", "", None)
    }

  @Test
  def deleteFolder_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "create-media": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["deleteMedia"]
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createMediaController(roleModel)

    for {
      folderId <- insertTestFolder()
      _ <- controller.deleteFolder(folderId)
    } yield ()
  }

  @Test
  def deleteFolder_notAuthorized_throwsException(implicit c: TestContext): Unit =
    okTest {

      val controller = createMediaController()

      for {
        folderId <- insertTestFolder()
        ex <- controller.deleteFolder(folderId).recover({ case ex => ex })
      } yield {
        assertEquals(UnauthorizedException(DeleteMedia, Seq()), ex)
      }
    }

  @Test
  def createFile_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "create-media": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["createMedia"]
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createMediaController(roleModel)
    controller.addFile(
      MultiLanguageValue("de" -> "file"),
      MultiLanguageValue.empty(),
      MultiLanguageValue("de" -> "file.pdf"),
      None
    )
  }

  @Test
  def createFile_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {

      val controller = createMediaController()
      controller.addFile(
        MultiLanguageValue("de" -> "file"),
        MultiLanguageValue.empty(),
        MultiLanguageValue("de" -> "file.pdf"),
        None
      )
    }

  @Test
  def deleteFile_authorized_ok(implicit c: TestContext): Unit =
    okTest {
      val roleModel = initRoleModel("""
                                      |{
                                      |  "delete-media": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["deleteMedia"]
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createMediaController(roleModel)

      for {
        file <- insertTestFile()
        _ <- controller.deleteFile(file.uuid)
      } yield ()
    }

  @Test
  def deleteFile_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createMediaController()

      for {
        file <- insertTestFile()
        _ <- controller.deleteFile(file.uuid)
      } yield ()
    }

  @Test
  def deleteFileLang_authorized_ok(implicit c: TestContext): Unit =
    okTest {
      val roleModel = initRoleModel("""
                                      |{
                                      |  "delete-media": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["deleteMedia"]
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createMediaController(roleModel)

      for {
        file <- insertTestFile()
        _ <- controller.deleteFile(file.uuid, "de")
      } yield ()
    }

  @Test
  def deleteFileLang_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createMediaController()

      for {
        file <- insertTestFile()
        _ <- controller.deleteFile(file.uuid, "de")
      } yield ()
    }

  @Test
  def uploadFile_authorized_ok(implicit c: TestContext): Unit =
    okTest {
      val controller = createMediaController()

      val fileName = "Scr$en Shot.pdf"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val mimeType = "application/pdf"

      for {

        file <- sendRequest("POST", "/files", Json.obj("title" -> Json.obj("de" -> "Ein Titel")))
        _ <- uploadFile("PUT", s"/files/${file.getString("uuid")}/de", filePath, mimeType)
      } yield ()
    }

  @Test
  def updateFile_authorized_ok(implicit c: TestContext): Unit =
    okTest {
      val roleModel = initRoleModel("""
                                      |{
                                      |  "edit-media": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["editMedia"]
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createMediaController(roleModel)

      for {
        file <- insertTestFile()
        _ <- controller.changeFile(
          file.uuid,
          MultiLanguageValue.empty(),
          MultiLanguageValue.empty(),
          MultiLanguageValue.empty(),
          MultiLanguageValue.empty(),
          MultiLanguageValue.empty(),
          None
        )
      } yield ()
    }

  @Test
  def updateFile_notAuthorized_throwsException(implicit c: TestContext): Unit =
    okTest {
      val controller = createMediaController()

      for {
        file <- insertTestFile()
        ex <- controller
          .changeFile(
            file.uuid,
            MultiLanguageValue.empty(),
            MultiLanguageValue.empty(),
            MultiLanguageValue.empty(),
            MultiLanguageValue.empty(),
            MultiLanguageValue.empty(),
            None
          )
          .recover({ case ex => ex })
      } yield {
        assertEquals(UnauthorizedException(EditMedia, Seq()), ex)
      }
    }

  @Test
  def updateFolder_authorized_ok(implicit c: TestContext): Unit =
    okTest {
      val roleModel = initRoleModel("""
                                      |{
                                      |  "edit-media": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["editMedia"]
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createMediaController(roleModel)

      for {
        folderId <- insertTestFolder()
        _ <- controller.changeFolder(folderId, "newName", "newDescription", None)
      } yield ()
    }

  @Test
  def updateFolder_notAuthorized_throwsException(implicit c: TestContext): Unit =
    okTest {
      val controller = createMediaController()

      for {
        folderId <- insertTestFolder()
        ex <- controller.changeFolder(folderId, "newName", "newDescription", None).recover({ case ex => ex })
      } yield {
        assertEquals(UnauthorizedException(EditMedia, Seq()), ex)
      }
    }

  @Test
  def mergeFile_authorized_ok(implicit c: TestContext): Unit =
    okTest {
      val roleModel = initRoleModel("""
                                      |{
                                      |  "edit-media": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["editMedia"]
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createMediaController(roleModel)

      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      val putFile1 = Json.obj(
        "title" -> Json.obj("de" -> "Test PDF 1"),
        "description" -> Json.obj("de" -> "A description about that PDF. 1")
      )

      for {
        fileUuid1 <- createFile("de", file, mimetype, None).map(_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid1", putFile1)
        fileUuid2 <- createFile("en", file, mimetype, None).map(_.getString("uuid"))
        _ <- sendRequest("PUT", s"/files/$fileUuid2", putFile1)

        _ <- controller.mergeFile(UUID.fromString(fileUuid1), "de", UUID.fromString(fileUuid2))
      } yield ()
    }

  @Test
  def mergeFile_notAuthorized_throwsException(implicit c: TestContext): Unit =
    okTest {
      val controller = createMediaController()

      val fileName = "Scr$en Shot.pdf"
      val file = s"/com/campudus/tableaux/uploads/$fileName"
      val mimetype = "application/pdf"

      val putFile1 = Json.obj("title" -> Json.obj("de" -> "Test PDF 1"))
      val putFile2 = Json.obj("title" -> Json.obj("en" -> "Test PDF 2"))

      for {
        file1 <- insertTestFile()
        file2 <- insertTestFile()
        ex <- controller.mergeFile(file1.uuid, "de", file2.uuid).recover({ case ex => ex })
      } yield {
        assertEquals(UnauthorizedException(EditMedia, Seq()), ex)
      }
    }
}
