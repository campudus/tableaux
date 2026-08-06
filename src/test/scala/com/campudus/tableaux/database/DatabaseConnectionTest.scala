package com.campudus.tableaux.database

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.helper.Json
import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.testtools.{TestAssertionHelper, TestConfig}

import io.vertx.core.Vertx
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.json.JsonObject
import io.vertx.scala.SQLConnection

import scala.concurrent.Future
import scala.util.{Failure, Success}

import org.junit.{Ignore, Test}
import org.junit.Assert._
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
@Ignore
class DatabaseConnectionTest extends VertxAccess with TestConfig with TestAssertionHelper {

  override val vertx: Vertx = Vertx.vertx()

  def okTest(f: => Future[?])(implicit context: TestContext): Unit = {
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

  private def withDatabaseConnection[A](f: DatabaseConnection => Future[A])(implicit context: TestContext): Unit = {
    val config = fileConfig.getJsonObject("database", Json.obj())
    val sqlConnection = SQLConnection(this, config)
    val databaseConnection = DatabaseConnection(this, sqlConnection)

    okTest(f(databaseConnection))
  }

  @Test
  def testJsonbScalarValuesComeBackAsJsonText(implicit context: TestContext): Unit = withDatabaseConnection {
    databaseConnection =>
      for {
        result <- databaseConnection.query("SELECT 'true'::jsonb, '42'::jsonb, '\"hello\"'::jsonb")
      } yield {
        val row = result.getJsonArray("results").getJsonArray(0)
        assertEquals("true", row.getString(0))
        assertEquals("42", row.getString(1))
        assertEquals("\"hello\"", row.getString(2))
      }
  }

  @Test
  def testPlainBooleanColumnKeepsNativeType(implicit context: TestContext): Unit = withDatabaseConnection {
    databaseConnection =>
      for {
        result <- databaseConnection.query("SELECT true::boolean, false::boolean")
      } yield {
        val row = result.getJsonArray("results").getJsonArray(0)
        assertEquals(true, row.getBoolean(0))
        assertEquals(false, row.getBoolean(1))
      }
  }

  @Test
  def testJsonbBindReparsesBareBooleanScalar(implicit context: TestContext): Unit = withDatabaseConnection {
    databaseConnection =>
      for {
        // jsonb_typeof reports the type Postgres actually stored, independent of how normalizeValue re-stringifies
        // it on the way back out - this is what pins down that toBindValue re-decodes "true" into a real JSON
        // boolean before binding, instead of leaving it as a Java String that pg-client would then double-encode
        // into the JSON string "true".
        result <- databaseConnection.query("SELECT jsonb_typeof(?::jsonb)", Json.arr("true"))
      } yield {
        assertEquals("boolean", result.getJsonArray("results").getJsonArray(0).getString(0))
      }
  }

  @Test
  def testJsonCastGateDoesNotCorruptUnrelatedPlainParams(implicit context: TestContext): Unit = withDatabaseConnection {
    databaseConnection =>
      for {
        // hasJsonCast is computed once per statement, so this second parameter - plain text bound against a
        // ::varchar cast, not ::jsonb - is still routed through the same JSON-decode attempt as the first. It must
        // come back unchanged rather than getting reinterpreted as a JSON number.
        result <- databaseConnection.query("SELECT ?::jsonb, ?::varchar", Json.arr("true", "123"))
      } yield {
        val row = result.getJsonArray("results").getJsonArray(0)
        assertEquals("true", row.getString(0))
        assertEquals("123", row.getString(1))
      }
  }

  var host: String = scala.compiletime.uninitialized
  var port: Int = scala.compiletime.uninitialized
  var databaseConfig: JsonObject = scala.compiletime.uninitialized
  var authConfig: JsonObject = scala.compiletime.uninitialized
  var cdnConfig: JsonObject = scala.compiletime.uninitialized
  var thumbnailsConfig: JsonObject = scala.compiletime.uninitialized
  var tableauxConfig: TableauxConfig = scala.compiletime.uninitialized
}
