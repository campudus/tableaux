package com.campudus.tableaux

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.File
import com.campudus.tableaux.database.model.{FileModel, TableauxModel}
import org.junit.Test
import org.vertx.testtools.VertxAssert._

import scala.concurrent.Future

class FileTest extends TableauxTestBase {

  def createFileModel(): FileModel = {
    val dbConnection = DatabaseConnection(tableauxConfig)
    val model = TableauxModel(dbConnection)

    new FileModel(dbConnection)
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
}
