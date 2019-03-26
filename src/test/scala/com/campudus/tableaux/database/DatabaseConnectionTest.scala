package com.campudus.tableaux.database

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.testtools.{TestAssertionHelper, TestConfig}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import io.vertx.scala.core.Vertx
import org.junit.runner.RunWith
import org.junit.{Ignore, Test}
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future
import scala.util.{Failure, Success}

@RunWith(classOf[VertxUnitRunner])
@Ignore
class DatabaseConnectionTest extends VertxAccess with TestConfig with TestAssertionHelper {

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
  def testStatementAfterTimedOutStatement(implicit context: TestContext): Unit = {

    val config = fileConfig.getJsonObject("database", Json.obj())

    config.put("queryTimeout", 5000L)

    val sqlConnection = SQLConnection(this, config)
    val databaseConnection = DatabaseConnection(this, sqlConnection)

    okTest {
      val start = System.currentTimeMillis()

      val longRunningQueries = Future
        .sequence(Range(1, 20).map(index => {
          databaseConnection
            .query("SELECT ?::int, pg_sleep(11)", Json.arr(index))
            .recover({
              case _ => Json.obj("error" -> "timeout")
            })
        }))

      for {
        result <- databaseConnection.query("SELECT 'hello' AS test1, ?::varchar AS test2", Json.arr("world"))

        _ <- longRunningQueries

        _ = logger.info(s"long running select done ${System.currentTimeMillis() - start}")
      } yield {
        assertEquals(Json.arr("hello", "world"), result.getJsonArray("results").getJsonArray(0))
      }
    }
  }

  @Test
  def testStatementAfterTimedOutTransaction(implicit context: TestContext): Unit = {
    val config = fileConfig.getJsonObject("database", Json.obj())

    config.put("queryTimeout", 5000L)

    val sqlConnection = SQLConnection(this, config)
    val databaseConnection = DatabaseConnection(this, sqlConnection)

    okTest {
      databaseConnection
        .transactional({ transaction =>
          transaction.query("SELECT ?::int, pg_sleep(11)", Json.arr(1))
        })
        .recover({
          case _ => Json.obj("timeout" -> 1)
        })
        .flatMap({ resultTimeout =>
          assertEquals(Json.obj("timeout" -> 1), resultTimeout)

          databaseConnection
            .query("SELECT 'hello' AS test1, ?::varchar AS test2", Json.arr("world"))
            .map(result => {
              assertEquals(Json.arr("hello", "world"), result.getJsonArray("results").getJsonArray(0))
              result
            })
        })
    }
  }

  override var host: String = _
  override var port: Int = _
  override var databaseConfig: JsonObject = _
  override var tableauxConfig: TableauxConfig = _
}
