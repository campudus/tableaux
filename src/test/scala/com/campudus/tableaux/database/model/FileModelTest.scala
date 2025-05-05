package com.campudus.tableaux.database.model

import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.MultiLanguageValue
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class FileModelTest extends TableauxTestBase {

  private def createMediaController(): MediaController = {
    implicit val roleModel = RoleModel(tableauxConfig.rolePermissions)
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val folderModel = FolderModel(dbConnection)
    val fileModel = FileModel(dbConnection)
    val attachmentModel = AttachmentModel(dbConnection)
    val structureModel = StructureModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel, tableauxConfig)

    setRequestRoles(Seq("dev"))

    MediaController(tableauxConfig, folderModel, fileModel, attachmentModel, roleModel, tableauxModel)
  }

  def createFileModel: FileModel = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

    FileModel(dbConnection)
  }

  @Test
  def testCreateAndUpdateOfFile(implicit c: TestContext): Unit = {
    okTest {
      val model = createFileModel

      for {
        insertedFile <- model.add(
          MultiLanguageValue("de-DE" -> "Test 1"),
          MultiLanguageValue.empty(),
          MultiLanguageValue("de-DE" -> "external1.pdf"),
          None
        )

        sizeAfterAdd <- model.size()

        retrievedFile <- model.retrieve(insertedFile.uuid, withTmp = true)

        updatedFile <- model.update(
          insertedFile.uuid,
          title = MultiLanguageValue("de-DE" -> "Changed 1"),
          description = MultiLanguageValue("de-DE" -> "Changed 1"),
          internalName = insertedFile.internalName,
          externalName = MultiLanguageValue("de-DE" -> "external1.pdf"),
          folder = None,
          mimeType = insertedFile.mimeType
        )

        allFiles <- model.retrieveAll()

        size <- model.size()

        _ <- model.deleteById(insertedFile.uuid)

        sizeAfterDeleteOne <- model.size()
      } yield {
        assertEquals(0L, sizeAfterAdd)

        assertEquals(insertedFile, retrievedFile)

        assertFalse(insertedFile.updatedAt.isDefined)

        assertTrue(insertedFile.createdAt.isDefined)
        assertTrue(retrievedFile.createdAt.isDefined)
        assertTrue(updatedFile.createdAt.isDefined)

        assertTrue(updatedFile.updatedAt.isDefined)

        assertTrue(updatedFile.updatedAt.get.isAfter(updatedFile.createdAt.get))

        assertEquals(size, allFiles.size)

        assertEquals(updatedFile, allFiles.head)

        assertEquals(0L, sizeAfterDeleteOne)
      }
    }
  }

  @Test
  def testChangeToInvalidInternalName(implicit c: TestContext): Unit = {
    exceptionTest("error.request.invalid") {
      val controller = createMediaController()

      for {
        insertedFile <- controller.addFile(
          MultiLanguageValue("de-DE" -> "Test 1"),
          MultiLanguageValue.empty(),
          MultiLanguageValue("de-DE" -> "external1.pdf"),
          None
        )

        _ <- controller.changeFile(
          insertedFile.uuid,
          title = MultiLanguageValue("de-DE" -> "Changed 1"),
          description = MultiLanguageValue("de-DE" -> "Changed 1"),
          internalName = MultiLanguageValue("de-DE" -> "invalid.png"),
          externalName = MultiLanguageValue("de-DE" -> "external1.pdf"),
          folder = None,
          mimeType = insertedFile.mimeType
        )

      } yield ()
    }
  }
}
