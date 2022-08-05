package com.campudus.tableaux.helper

import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner

import scala.reflect.io.Path

import java.io.FileNotFoundException
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class FileUtilsTest extends TableauxTestBase {

  @Test
  def testMakeSameDirectoriesTwiceShouldNotFail(implicit context: TestContext): Unit = {
    okTest {
      val uploadsDirectory = fileConfig.getString("uploadsDirectory")
      val path = Path(uploadsDirectory) / "test" / "asdf"

      for {
        _ <- FileUtils(this.vertxAccess()).mkdirs(path)

        _ <- FileUtils(this.vertxAccess()).mkdirs(path)

        exists <- vertx.fileSystem().existsFuture(path.toString())

        _ <- vertx.fileSystem().deleteFuture(path.toString())
        _ <- vertx.fileSystem().deleteFuture(path.parent.toString())
      } yield {
        assertTrue(exists)
      }
    }
  }

  @Test
  def testLoadNonExistingJsonFileShouldFail(implicit context: TestContext): Unit = {
    okTest {
      val jsonFilePath = "./does_not_exist.json"

      for {
        isException: Boolean <- FileUtils(this.vertxAccess())
          .asyncReadJsonFile(jsonFilePath)
          .map(_ => false)
          .recover({
            case _: FileNotFoundException => true
          })
      } yield {
        assertEquals(isException, true)
      }
    }
  }

  @Test
  def testLoadJsonFileShouldNotFail(implicit context: TestContext): Unit = {
    okTest {
      val jsonFilePath = "./conf-example.json"

      for {
        json <- FileUtils(this.vertxAccess()).asyncReadJsonFile(jsonFilePath)
      } yield {
        assertEquals(json.getInteger("port"), 8080)
      }
    }
  }
}
