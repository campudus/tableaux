package com.campudus.tableaux.database.model

import com.campudus.tableaux.TableauxTestBase
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.Folder
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith

import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
class FolderModelTest extends TableauxTestBase {

  def createFolderModel(): FolderModel = {
    val sqlConnection = SQLConnection(verticle, databaseConfig)
    val dbConnection = DatabaseConnection(verticle, sqlConnection)

    new FolderModel(dbConnection)
  }

  @Test
  def testFolderModel(implicit c: TestContext): Unit = okTest {
    for {
      model <- Future.successful(createFolderModel())

      insertedFolder1 <- model.add(name = "hallo", description = "Test", parent = None)
      insertedFolder2 <- model.add(name = "hallo", description = "Test", parent = None)

      retrievedFolder <- model.retrieve(insertedFolder1.id)
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
}
