package com.campudus.tableaux.api.auth

import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.{DomainObject, MultiLanguageValue}
import com.campudus.tableaux.database.model.{AttachmentModel, FileModel, FolderModel}
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.JSONCompareMode
import org.vertx.scala.core.json.{Json, JsonObject}

trait MediaControllerAuthTestBase extends TableauxTestBase {

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

  def getPermission(domainObject: DomainObject): JsonObject = {
    domainObject.getJson.getJsonObject("permission")
  }
}

@RunWith(classOf[VertxUnitRunner])
class MediaControllerAuthTest_checkAuthorization extends MediaControllerAuthTestBase {

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

@RunWith(classOf[VertxUnitRunner])
class MediaControllerAuthTest_enrichAuthorization extends MediaControllerAuthTestBase {

  @Test
  def enrich_noPermission_noActionIsAllowed(implicit c: TestContext): Unit =
    okTest {
      val controller = createMediaController()

      for {
        permission <- controller.retrieveRootFolder("de-DE").map(getPermission)
      } yield {

        val expected = Json.obj(
          "create" -> false,
          "edit" -> false,
          "delete" -> false
        )

        assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
      }
    }

  @Test
  def enrich_createGranted_onlyCreateIsAllowed(implicit c: TestContext): Unit = {

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

    okTest {
      for {
        permission <- controller.retrieveRootFolder("de-DE").map(getPermission)
      } yield {

        val expected = Json.obj("create" -> true)

        assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def enrich_editAndDeleteGranted_onlyEditAndDeleteAreAllowed(implicit c: TestContext): Unit = {

    setRequestRoles("edit-delete-media")

    val roleModel = RoleModel("""
                                |{
                                |  "edit-delete-media": [
                                |    {
                                |      "type": "grant",
                                |      "action": ["edit", "delete"],
                                |      "scope": "media"
                                |    }
                                |  ]
                                |}""".stripMargin)

    val controller = createMediaController(roleModel)

    okTest {
      for {
        permission <- controller.retrieveRootFolder("de-DE").map(getPermission)
      } yield {

        val expected = Json.obj(
          "create" -> false,
          "edit" -> true,
          "delete" -> true
        )

        assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def enrich_editAndDeleteGranted_deleteDenied_onlyEditIsAllowed(implicit c: TestContext): Unit = {

    setRequestRoles("edit-delete-media", "NOT-delete-media")

    val roleModel = RoleModel("""
                                |{
                                |  "edit-delete-media": [
                                |    {
                                |      "type": "grant",
                                |      "action": ["edit", "delete"],
                                |      "scope": "media"
                                |    }
                                |  ],
                                |  "NOT-delete-media": [
                                |    {
                                |      "type": "deny",
                                |      "action": ["delete"],
                                |      "scope": "media"
                                |    }
                                |  ]
                                |}""".stripMargin)

    val controller = createMediaController(roleModel)

    okTest {
      for {
        permission <- controller.retrieveRootFolder("de-DE").map(getPermission)
      } yield {

        val expected = Json.obj(
          "create" -> false,
          "edit" -> true,
          "delete" -> false
        )

        assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
      }
    }
  }

}
