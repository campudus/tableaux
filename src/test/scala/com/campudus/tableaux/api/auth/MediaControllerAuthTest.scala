package com.campudus.tableaux.api.auth

import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.MultiLanguageValue
import com.campudus.tableaux.database.model.{AttachmentModel, FileModel, FolderModel}
import com.campudus.tableaux.router.auth.RoleModel
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class MediaControllerAuthTest extends TableauxTestBase {

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
    fileModel.add(MultiLanguageValue("de-DE" -> "file"),
                  MultiLanguageValue.empty(),
                  MultiLanguageValue("de-DE" -> "file.pdf"),
                  None)
  }

  @Test
  def createFolder_authorized_ok(implicit c: TestContext): Unit = okTest {
    setRequestRoles("create-media")

    val roleModel = RoleModel("""
                                |{
                                |  "create-media": [
                                |    {
                                |      "type": "grant",
                                |      "action": ["create"],
                                |      "scope": "media"
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
  def createFile_authorized_ok(implicit c: TestContext): Unit = okTest {

    setRequestRoles("create-media")

    val roleModel = RoleModel("""
                                |{
                                |  "create-media": [
                                |    {
                                |      "type": "grant",
                                |      "action": ["create"],
                                |      "scope": "media"
                                |    }
                                |  ]
                                |}""".stripMargin)

    val controller = createMediaController(roleModel)
    controller.addFile(MultiLanguageValue("de-DE" -> "file"),
                       MultiLanguageValue.empty(),
                       MultiLanguageValue("de-DE" -> "file.pdf"),
                       None)
  }

  @Test
  def createFile_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {

      val controller = createMediaController()
      controller.addFile(MultiLanguageValue("de-DE" -> "file"),
                         MultiLanguageValue.empty(),
                         MultiLanguageValue("de-DE" -> "file.pdf"),
                         None)
    }

  @Test
  def deleteFile_authorized_ok(implicit c: TestContext): Unit =
    okTest {
      setRequestRoles("delete-media")

      val roleModel = RoleModel("""
                                  |{
                                  |  "delete-media": [
                                  |    {
                                  |      "type": "grant",
                                  |      "action": ["delete"],
                                  |      "scope": "media"
                                  |    }
                                  |  ]
                                  |}""".stripMargin)

      val controller = createMediaController(roleModel)

      for {
        file <- insertTestFile
        _ <- controller.deleteFile(file.uuid)
      } yield ()
    }

  @Test
  def deleteFile_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createMediaController()

      for {
        file <- insertTestFile
        _ <- controller.deleteFile(file.uuid)
      } yield ()
    }

  @Test
  def deleteFileLang_authorized_ok(implicit c: TestContext): Unit =
    okTest {
      setRequestRoles("delete-media")

      val roleModel = RoleModel("""
                                  |{
                                  |  "delete-media": [
                                  |    {
                                  |      "type": "grant",
                                  |      "action": ["delete"],
                                  |      "scope": "media"
                                  |    }
                                  |  ]
                                  |}""".stripMargin)

      val controller = createMediaController(roleModel)

      for {
        file <- insertTestFile
        _ <- controller.deleteFile(file.uuid, "de-DE")
      } yield ()
    }

  @Test
  def deleteFileLang_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createMediaController()

      for {
        file <- insertTestFile
        _ <- controller.deleteFile(file.uuid, "de-DE")
      } yield ()
    }
}

// insertedFile <- model.add(MultiLanguageValue("de-DE" -> "Test 1"),
// MultiLanguageValue.empty(),
// MultiLanguageValue("de-DE" -> "external1.pdf"),
// None)
//
// sizeAfterAdd <- model.size()
//
// retrievedFile <- model.retrieve(insertedFile.uuid, withTmp = true)
//
// updatedFile <- model.update(
// insertedFile.uuid,
// title = MultiLanguageValue("de-DE" -> "Changed 1"),
// description = MultiLanguageValue("de-DE" -> "Changed 1"),
// internalName = insertedFile.internalName,
// externalName = MultiLanguageValue("de-DE" -> "external1.pdf"),
// folder = None,
// mimeType = insertedFile.mimeType
// )
