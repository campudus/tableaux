package com.campudus.tableaux

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.{File, Folder}
import com.campudus.tableaux.database.model.{FileModel, FolderModel}
import org.junit.Test
import org.vertx.testtools.VertxAssert._

import scala.concurrent.Future

class FileTest extends TableauxTestBase {

  def createFileModel(): FileModel = {
    val dbConnection = DatabaseConnection(tableauxConfig)

    new FileModel(dbConnection)
  }

  def createFolderModel(): FolderModel = {
    val dbConnection = DatabaseConnection(tableauxConfig)

    new FolderModel(dbConnection)
  }

  @Test
  def testModel(): Unit = okTest {
    val file = File("lulu", "lala", "llu")

    for {
      model <- Future.successful(createFileModel())

      insertedFile1 <- model.add(file)
      insertedFile2 <- model.add(file)

      retrievedFile <- model.retrieve(insertedFile1.uuid.get)
      updatedFile <- model.update(File(retrievedFile.uuid.get, "blub", "flab", "test"))

      allFiles <- model.retrieveAll()

      size <- model.size()
      _ <- model.delete(insertedFile1)
      sizeAfterDelete <- model.size()
    } yield {
      assertEquals(insertedFile1, retrievedFile)

      assert(retrievedFile.updatedAt.isEmpty)
      assert(updatedFile.createdAt.isDefined)
      assert(updatedFile.updatedAt.isDefined)

      assert(updatedFile.updatedAt.get.isAfter(updatedFile.createdAt.get))

      assertEquals(2, allFiles.toList.size)

      assertEquals(2, size)
      assertEquals(1, sizeAfterDelete)
    }
  }

  @Test
  def testFolder(): Unit = okTest {
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
}
