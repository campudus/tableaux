package com.campudus.tableaux.helper

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.testtools.{TestAssertionHelper, TestConfig}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.core.Vertx
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.JsonObject

import scala.concurrent.Future
import scala.reflect.io.Path
import scala.util.{Failure, Success}

@RunWith(classOf[VertxUnitRunner])
class FileUtilsTest extends VertxAccess with TestConfig with TestAssertionHelper {

  override val vertx: Vertx = Vertx.vertx()

  def okTest(f: => Future[_])(implicit context: TestContext): Unit = {
    val async = context.async()
    (try {
      f
    } catch {
      case ex: Throwable => Future.failed(ex)
    }) onComplete {
      case Success(_) => async.complete()
      case Failure(ex) =>
        logger.error("failed test", ex)
        context.fail(ex)
        async.complete()
    }
  }

  @Test
  def testMakeSameDirectoriesTwiceShouldNotFail(implicit context: TestContext): Unit = {
    okTest {
      val uploadsDirectory = fileConfig.getString("uploadsDirectory")
      val path = Path(uploadsDirectory) / "test" / "asdf"

      for {
        _ <- FileUtils(this).mkdirs(path)

        _ <- FileUtils(this).mkdirs(path)

        exists <- vertx.fileSystem().existsFuture(path.toString())

        _ <- vertx.fileSystem().deleteFuture(path.toString())
        _ <- vertx.fileSystem().deleteFuture(path.parent.toString())
      } yield {
        assertTrue(exists)
      }
    }
  }

  override var host: String = _
  override var port: Int = _
  override var databaseConfig: JsonObject = _
  override var authConfig: JsonObject = _
  override var tableauxConfig: TableauxConfig = _
}
