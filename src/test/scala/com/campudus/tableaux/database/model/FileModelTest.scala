package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.TableauxTestBase
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.{MultiLanguageValue, TableauxFile}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith

import scala.concurrent.Future
import scala.util.Random

@RunWith(classOf[VertxUnitRunner])
class FileModelTest extends TableauxTestBase {

  def createFileModel(): FileModel = {
    val sqlConnection = SQLConnection(verticle, databaseConfig)
    val dbConnection = DatabaseConnection(verticle, sqlConnection)

    new FileModel(dbConnection)
  }

  @Test
  def testFileModel(implicit c: TestContext): Unit = okTest {
    def file = TableauxFile(
      UUID.randomUUID(),
      None,
      MultiLanguageValue("de_DE" -> "Test"),
      MultiLanguageValue.empty(),
      MultiLanguageValue("de_DE" -> s"internal${Random.nextInt()}.pdf"),
      MultiLanguageValue("de_DE" -> "external.pdf"),
      MultiLanguageValue("de_DE" -> "application/pdf"),
      None,
      None
    )

    for {
      model <- Future.successful(createFileModel())

      tempFile1 <- model.add(file)
      tempFile2 <- model.add(file)

      sizeAfterAdd <- model.size()

      insertedFile1 <- model.update(tempFile1)
      insertedFile2 <- model.update(tempFile2)

      retrievedFile <- model.retrieve(insertedFile1.uuid)
      updatedFile <- model.update(TableauxFile(uuid = retrievedFile.uuid, MultiLanguageValue("de_DE" -> "Changed"), MultiLanguageValue("de_DE" -> "Changed"), MultiLanguageValue("de_DE" -> "external.pdf"), None))

      allFiles <- model.retrieveAll()

      size <- model.size()

      _ <- model.delete(insertedFile1)

      sizeAfterDeleteOne <- model.size()
    } yield {
      assertEquals(0L, sizeAfterAdd)

      assertEquals(insertedFile1, retrievedFile)

      assert(retrievedFile.updatedAt.isDefined)

      assert(updatedFile.createdAt.isDefined)
      assert(updatedFile.updatedAt.isDefined)

      assert(updatedFile.updatedAt.get.isAfter(updatedFile.createdAt.get))

      assertEquals(2, allFiles.toList.size)

      assertEquals(2L, size)
      assertEquals(1L, sizeAfterDeleteOne)
    }
  }
}
