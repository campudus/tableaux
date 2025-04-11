package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class FolderModelTest extends TableauxTestBase {

  private def createFolderModel(): FolderModel = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

    new FolderModel(dbConnection)
  }

  @Test
  def testFolderModel(implicit c: TestContext): Unit = okTest {
    for {
      model <- Future.successful(createFolderModel())

      insertedFolder1 <- model.add(name = "Hallo 1", description = "Test", parentId = None)
      insertedFolder2 <- model.add(name = "Hallo 2", description = "Test", parentId = None)

      retrievedFolder <- model.retrieve(insertedFolder1.id)
      updatedFolder <- model.update(retrievedFolder.id, name = "blub", description = "flab", parentId = None)

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
  def testRetrievingParentIds(implicit c: TestContext): Unit = okTest {
    for {
      model <- Future.successful(createFolderModel())

      root <- model.add(name = "root", description = "Test", parentId = None)

      root_lustig <- model.add(name = "root.lustig", description = "Test", parentId = Some(root.id))

      root_lustig_peter <- model.add(name = "root.lustig.peter", description = "Test", parentId = Some(root_lustig.id))
      root_lustig_sepp <- model.add(name = "root.lustig.sepp", description = "Test", parentId = Some(root_lustig.id))

      root_hallo <- model.add(name = "root.hallo", description = "Test", parentId = Some(root.id))

      root_hallo_mueller <- model.add(name = "root.hallo.müller", description = "Test", parentId = Some(root_hallo.id))

      root_hallo_mueller_folder <- model.retrieve(root_hallo_mueller.id)

      root_lustig_peter_folder <- model.retrieve(root_lustig_peter.id)
      root_lustig_sepp_folder <- model.retrieve(root_lustig_sepp.id)
    } yield {
      assertEquals(Seq(root.id, root_hallo.id), root_hallo_mueller_folder.parentIds)

      assertEquals(Seq(root.id, root_lustig.id), root_lustig_peter_folder.parentIds)
      assertEquals(Seq(root.id, root_lustig.id), root_lustig_sepp_folder.parentIds)
    }
  }
}
