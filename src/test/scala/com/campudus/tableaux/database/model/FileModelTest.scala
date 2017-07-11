package com.campudus.tableaux.database.model

import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.MultiLanguageValue
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class FileModelTest extends TableauxTestBase {

  private def createFileModel(): FileModel = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

    FileModel(dbConnection)
  }

  private def createFolderModel(): FolderModel = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

    FolderModel(dbConnection)
  }

  private def createAttachmentModel(): AttachmentModel = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

    AttachmentModel(dbConnection)
  }

  private def createMediaController(): MediaController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)

    MediaController(tableauxConfig, createFolderModel(), createFileModel(), createAttachmentModel())
  }

  @Test
  def testCreateAndUpdateOfFile(implicit c: TestContext): Unit = {
    okTest {
      val model = createFileModel()

      for {
        insertedFile <- model.add(MultiLanguageValue("de_DE" -> "Test 1"),
                                  MultiLanguageValue.empty(),
                                  MultiLanguageValue("de_DE" -> "external1.pdf"),
                                  None)

        sizeAfterAdd <- model.size()

        retrievedFile <- model.retrieve(insertedFile.uuid, withTmp = true)

        updatedFile <- model.update(
          insertedFile.uuid,
          title = MultiLanguageValue("de_DE" -> "Changed 1"),
          description = MultiLanguageValue("de_DE" -> "Changed 1"),
          internalName = insertedFile.internalName,
          externalName = MultiLanguageValue("de_DE" -> "external1.pdf"),
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
        insertedFile <- controller.addFile(MultiLanguageValue("de_DE" -> "Test 1"),
                                           MultiLanguageValue.empty(),
                                           MultiLanguageValue("de_DE" -> "external1.pdf"),
                                           None)

        _ <- controller.changeFile(
          insertedFile.uuid,
          title = MultiLanguageValue("de_DE" -> "Changed 1"),
          description = MultiLanguageValue("de_DE" -> "Changed 1"),
          internalName = MultiLanguageValue("de_DE" -> "invalid.png"),
          externalName = MultiLanguageValue("de_DE" -> "external1.pdf"),
          folder = None,
          mimeType = insertedFile.mimeType
        )

      } yield ()
    }
  }
}
