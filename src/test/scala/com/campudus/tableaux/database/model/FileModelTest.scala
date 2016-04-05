package com.campudus.tableaux.database.model

import com.campudus.tableaux.TableauxTestBase
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.MultiLanguageValue
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class FileModelTest extends TableauxTestBase {

  def createFileModel(): FileModel = {
    val sqlConnection = SQLConnection(verticle, databaseConfig)
    val dbConnection = DatabaseConnection(verticle, sqlConnection)

    new FileModel(dbConnection)
  }

  @Test
  def testFileModel(implicit c: TestContext): Unit = okTest {
    val model = createFileModel()

    for {
      insertedFile <- model.add(MultiLanguageValue("de_DE" -> "Test 1"), MultiLanguageValue.empty(), MultiLanguageValue("de_DE" -> "external1.pdf"), None)

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
