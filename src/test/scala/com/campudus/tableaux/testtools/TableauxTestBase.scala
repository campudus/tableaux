package com.campudus.tableaux.testtools

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.DomainObject
import com.campudus.tableaux.database.model.SystemModel
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.testtools.RequestCreation.ColumnType
import com.campudus.tableaux.{CustomException, Starter, TableauxConfig}
import com.typesafe.scalalogging.LazyLogging
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.{ScalaVerticle, VertxExecutionContext}
import io.vertx.scala.FutureHelper._
import io.vertx.scala.SQLConnection
import io.vertx.scala.core.file.{AsyncFile, OpenOptions}
import io.vertx.scala.core.http._
import io.vertx.scala.core.streams.Pump
import io.vertx.scala.core.{DeploymentOptions, Vertx}
import org.junit.runner.RunWith
import org.junit.{After, Before}
import org.skyscreamer.jsonassert.{JSONAssert, JSONCompareMode}
import org.vertx.scala.core.json.{JsonObject, _}

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

case class TestCustomException(message: String, id: String, statusCode: Int) extends Throwable {

  override def toString: String = s"TestCustomException(status=$statusCode,id=$id,message=$message)"
}

trait TestAssertionHelper {

  def fail[A](message: String)(implicit c: TestContext): Unit = {
    c.fail(message)
  }

  def assertEquals[A](message: String, expected: A, actual: A)(implicit c: TestContext): TestContext = {
    c.assertEquals(expected, actual, message)
  }

  def assertEquals[A](expected: A, actual: A)(implicit c: TestContext): TestContext = {
    c.assertEquals(expected, actual)
  }

  def assertContains(expected: JsonObject, actual: JsonObject)(implicit c: TestContext): TestContext = {
    expected
      .fieldNames()
      .asScala
      .map(
        key => assertEquals(s"path [.$key]", expected.getValue(key), actual.getValue(key))
      )

    c
  }

  def assertJSONEquals(expected: JsonObject, actual: JsonObject, compareMode: JSONCompareMode) {
    JSONAssert.assertEquals(expected.toString, actual.toString, compareMode)
  }

  def assertJSONEquals(expected: String, actual: String, compareMode: JSONCompareMode = JSONCompareMode.LENIENT) {
    JSONAssert.assertEquals(expected, actual, compareMode)
  }

  def assertContainsDeep(expected: JsonArray, actual: JsonArray)(implicit c: TestContext): TestContext =
    assertContainsDeep(expected, actual, path = "")

  def assertContainsDeep(expected: JsonArray, actual: JsonArray, path: String)(implicit c: TestContext): TestContext = {
    assertEquals(s"expected size is ${expected.size()} != ${actual.size()}, path [$path[]]",
                 expected.size(),
                 actual.size())

    expected.asScala
      .zip(actual.asScala)
      .zipWithIndex
      .map({
        case ((expectedArr: JsonArray, actualArr: JsonArray), index) =>
          assertContainsDeep(expectedArr, actualArr, s"$path[$index]")

        case ((expectedObj: JsonObject, actualObj: JsonObject), index) =>
          assertContainsDeep(expectedObj, actualObj, s"$path[$index]")

        case ((expectedVal, actualVal), index) =>
          assertEquals(s"path [$path[$index]]", expectedVal, actualVal)
      })

    c
  }

  def assertContainsDeep(expected: JsonObject, actual: JsonObject)(implicit c: TestContext): TestContext =
    assertContainsDeep(expected, actual, path = "")

  def assertContainsDeep(expected: JsonObject, actual: JsonObject, path: String)(
      implicit c: TestContext): TestContext = {

    expected
      .fieldNames()
      .asScala
      .map(key => ((expected.getValue(key), actual.getValue(key)), key))
      .map({
        case ((expectedObj: JsonObject, actualObj: JsonObject), key) =>
          assertContainsDeep(expectedObj, actualObj, s"$path.$key")

        case ((expectedArr: JsonArray, actualArr: JsonArray), key) =>
          assertContainsDeep(expectedArr, actualArr, s"$path.$key")

        case ((expectedVal, actualVal), key) =>
          assertEquals(s"path [$path.$key]", expectedVal, actualVal)
      })

    c
  }

  def assertNull(expected: Any)(implicit c: TestContext): TestContext = {
    c.assertNull(expected)
  }

  def assertNotNull(expected: Any)(implicit c: TestContext): TestContext = {
    c.assertNotNull(expected)
  }

  def assertTrue(message: String, condition: Boolean)(implicit c: TestContext): TestContext = {
    c.assertTrue(condition, message)
  }

  def assertTrue(condition: Boolean)(implicit c: TestContext): TestContext = {
    c.assertTrue(condition)
  }

  def assertFalse(condition: Boolean)(implicit c: TestContext): TestContext = {
    c.assertFalse(condition)
  }

  def assertFalse(message: String, condition: Boolean)(implicit c: TestContext): TestContext = {
    c.assertFalse(condition, message)
  }

  def assertNotSame[A](first: A, second: A)(implicit c: TestContext): TestContext = {
    c.assertNotEquals(first, second)
  }
}

@RunWith(classOf[VertxUnitRunner])
trait TableauxTestBase
    extends TestConfig
    with LazyLogging
    with TestAssertionHelper
    with JsonCompatible
    with TestVertxAccess {

  override var vertx: Vertx = _

  override implicit var executionContext: VertxExecutionContext = _

  override var databaseConfig: JsonObject = _

  override var authConfig: JsonObject = _

  override var host: String = _

  override var port: Int = _

  override var tableauxConfig: TableauxConfig = _

  // default access token used for all integration tests if no explicit token is provided
  var wildcardAccessToken: String = _

  @Before
  def before(context: TestContext): Unit = {
    vertx = Vertx.vertx()

    executionContext = VertxExecutionContext(
      io.vertx.scala.core.Context(vertx.asJava.asInstanceOf[io.vertx.core.Vertx].getOrCreateContext())
    )

    val config = Json
      .fromObjectString(fileConfig.encode())
      .put("host", fileConfig.getString("host", "127.0.0.1"))
      .put("port", getFreePort)

    databaseConfig = config.getJsonObject("database", Json.obj())
    authConfig = config.getJsonObject("auth", Json.obj())

    host = config.getString("host")
    port = config.getInteger("port").intValue()

    tableauxConfig = new TableauxConfig(
      vertx,
      authConfig,
      databaseConfig,
      config.getString("workingDirectory"),
      config.getString("uploadsDirectory"),
    )

    val async = context.async()

    val options = DeploymentOptions()
      .setConfig(config)

    val completionHandler = {
      case Success(id) =>
        logger.info(s"Verticle deployed with ID $id")

        val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
        val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
        val system = SystemModel(dbConnection)

        for {
          _ <- system.uninstall()
          _ <- system.install()
        } yield async.complete()

      case Failure(e) =>
        logger.error("Verticle couldn't be deployed.", e)
        context.fail(e)
        async.complete()
    }: Try[String] => Unit

    vertx
      .deployVerticleFuture(ScalaVerticle.nameForVerticle[Starter], options)
      .onComplete(completionHandler)

    val tokenHelper = TokenHelper(this.vertxAccess())

    wildcardAccessToken = tokenHelper.generateToken(
      Json.obj("aud" -> "grud-backend", "iss" -> "campudus-test", "preferred_username" -> "Test"))
  }

  @After
  def after(context: TestContext): Unit = vertx.close(context.asyncAssertSuccess())

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

  def exceptionTest(id: String)(f: => Future[_])(implicit context: TestContext): Unit = {
    val async = context.async()
    f onComplete {
      case Success(_) =>
        val msg = s"Test with id $id should fail but got no exception."
        logger.error(msg)
        context.fail(msg)
        async.complete()
      case Failure(ex: TestCustomException) =>
        context.assertEquals(id, ex.id)
        async.complete()
      case Failure(ex: CustomException) =>
        context.assertEquals(id, ex.id)
        async.complete()
      case Failure(ex) =>
        val msg = s"Test with id $id failed but got wrong exception (${ex.getClass.toString}: ${ex.getMessage})."
        logger.error(msg)
        context.fail(msg)
        async.complete()
    }
  }

  def sendRequest(method: String, path: String): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p, None).end()
    p.future
  }

  def sendRequest(method: String, path: String, tokenOpt: Option[String]): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p, tokenOpt).end()
    p.future
  }

  def sendRequest(method: String, path: String, jsonObj: JsonObject): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p, None).end(jsonObj.encode())
    p.future
  }

  def sendRequest(method: String, path: String, jsonObj: JsonObject, tokenOpt: Option[String]): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p, tokenOpt).end(jsonObj.encode())
    p.future
  }

  def sendRequest(method: String, path: String, body: String): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p, None).end(body)
    p.future
  }

  def sendRequest(method: String, path: String, body: String, tokenOpt: Option[String]): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p, tokenOpt).end(body)
    p.future
  }

  def sendRequest(method: String, path: String, domainObject: DomainObject): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p, None).end(domainObject.getJson.encode())
    p.future
  }

  def sendRequest(method: String,
                  path: String,
                  domainObject: DomainObject,
                  tokenOpt: Option[String]): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p, tokenOpt).end(domainObject.getJson.encode())
    p.future
  }

  def sendStringRequest(method: String, path: String): Future[String] = {
    val p = Promise[String]()
    httpStringRequest(method, path, p, None).end()
    p.future
  }

  def sendStringRequest(method: String, path: String, tokenOpt: Option[String]): Future[String] = {
    val p = Promise[String]()
    httpStringRequest(method, path, p, tokenOpt).end()
    p.future
  }

  def sendStringRequest(method: String, path: String, jsonObj: JsonObject): Future[String] = {
    val p = Promise[String]()
    httpStringRequest(method, path, p, None).end(jsonObj.encode())
    p.future
  }

  def sendStringRequest(method: String, path: String, jsonObj: JsonObject, tokenOpt: Option[String]): Future[String] = {
    val p = Promise[String]()
    httpStringRequest(method, path, p, tokenOpt).end(jsonObj.encode())
    p.future
  }

  private def createJsonResponseHandler(p: Promise[JsonObject]): (HttpClient, HttpClientResponse) => Unit = {
    (client: HttpClient, resp: HttpClientResponse) =>
      def jsonBodyHandler(buf: Buffer): Unit = {
        val body = buf.toString()

        client.close()

        if (resp.statusCode() != 200) {
          p.failure(TestCustomException(body, resp.statusMessage(), resp.statusCode()))
        } else {
          try {
            p.success(Json.fromObjectString(body))
          } catch {
            case ex: Exception => p.failure(ex)
          }
        }
      }

      resp.bodyHandler(jsonBodyHandler(_: Buffer))
  }

  private def createStringResponseHandler(p: Promise[String]): (HttpClient, HttpClientResponse) => Unit = {
    (client: HttpClient, resp: HttpClientResponse) =>
      def stringBodyHandler(buf: Buffer): Unit = {
        val body = buf.toString()

        client.close()

        if (resp.statusCode() != 200) {
          p.failure(TestCustomException(body, resp.statusMessage(), resp.statusCode()))
        } else {
          try {
            p.success(body)
          } catch {
            case ex: Exception => p.failure(ex)
          }
        }
      }

      resp.bodyHandler(stringBodyHandler(_: Buffer))
  }

  private def createExceptionHandler[A](p: Promise[A]): (HttpClient, Throwable) => Unit = {
    (client: HttpClient, x: Throwable) =>
      client.close()
      p.failure(x)
  }
  private def httpStringRequest(method: String,
                                path: String,
                                p: Promise[String],
                                tokenOpt: Option[String]): HttpClientRequest = {
    httpRequest(method, path, createStringResponseHandler(p), createExceptionHandler[String](p), tokenOpt)
  }

  private def httpJsonRequest(method: String,
                              path: String,
                              p: Promise[JsonObject],
                              tokenOpt: Option[String]): HttpClientRequest = {
    httpRequest(method, path, createJsonResponseHandler(p), createExceptionHandler[JsonObject](p), tokenOpt)
  }

  def httpRequest(
      method: String,
      path: String,
      responseHandler: (HttpClient, HttpClientResponse) => Unit,
      exceptionHandler: (HttpClient, Throwable) => Unit,
      tokenOpt: Option[String]
  ): HttpClientRequest = {
    val _method = HttpMethod.valueOf(method.toUpperCase)

    val options = HttpClientOptions()
      .setKeepAlive(false)

    val client = vertx.createHttpClient(options)

    val token = tokenOpt.getOrElse(wildcardAccessToken)

    client
      .request(_method, port, host, path)
      .putHeader("Authorization", s"Bearer $token")
      .handler(responseHandler(client, _: HttpClientResponse))
      .exceptionHandler(exceptionHandler(client, _: Throwable))
  }

  protected def uploadFile(method: String, url: String, file: String, mimeType: String): Future[JsonObject] = {
    val filePath = getClass.getResource(file).toURI.getPath
    val fileName = file.substring(file.lastIndexOf("/") + 1)
    val boundary = "dLV9Wyq26L_-JQxk6ferf-RT153LhOO"
    val header =
      "--" + boundary + "\r\n" +
        "Content-Disposition: form-data; name=\"file\"; filename=\"" + fileName + "\"\r\n" +
        "Content-Type: " + mimeType + "\r\n\r\n"
    val footer = "\r\n--" + boundary + "--\r\n"
    val contentLength =
      String.valueOf(vertx.fileSystem.propsBlocking(filePath).size() + header.length + footer.length)

    futurify({ p: Promise[JsonObject] =>
      def requestHandler(req: HttpClientRequest): Unit = {
        req.putHeader("Content-length", contentLength)
        req.putHeader("Content-type", s"multipart/form-data; boundary=$boundary")

        logger.info(s"Loading file '$filePath' from disc, content-length=$contentLength")

        req.write(header)

        val asyncFile: Future[AsyncFile] =
          vertx.fileSystem().openFuture(filePath, OpenOptions())

        asyncFile.map({ file =>
          val pump = Pump.pump(file, req)

          file.exceptionHandler({ e: Throwable =>
            pump.stop()
            req.end("")
            p.failure(e)
          })

          file.endHandler({ _ =>
            file
              .closeFuture()
              .onComplete({
                case Success(_) =>
                  logger.info(s"File loaded, ending request, ${pump.numberPumped()} bytes pumped.")
                  req.end(footer)
                case Failure(e) =>
                  req.end("")
                  p.failure(e)
              })
          })

          pump.start()
        })
      }

      requestHandler(httpJsonRequest(method, url, p, None))
    })
  }

  protected def createDefaultColumns(tableId: TableId): Future[(ColumnId, ColumnId)] = {
    val createStringColumnJson =
      Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1", "identifier" -> true)))
    val createNumberColumnJson =
      Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))

    for {
      column1 <- sendRequest("POST", s"/tables/$tableId/columns", createStringColumnJson)
      columnId1 = column1.getJsonArray("columns").getJsonObject(0).getLong("id").toLong

      column2 <- sendRequest("POST", s"/tables/$tableId/columns", createNumberColumnJson)
      columnId2 = column1.getJsonArray("columns").getJsonObject(0).getLong("id").toLong
    } yield (columnId1, columnId2)
  }

  protected def createEmptyDefaultTable(
      name: String = "Test Table 1",
      tableNum: Int = 1,
      displayName: Option[JsonObject] = None,
      description: Option[JsonObject] = None
  ): Future[TableId] = {
    val postTable = Json.obj("name" -> name)

    displayName
      .map({ obj =>
        {
          Json.obj("displayName" -> obj)
        }
      })
      .foreach(postTable.mergeIn)

    description
      .map({ obj =>
        {
          Json.obj("description" -> obj)
        }
      })
      .foreach(postTable.mergeIn)

    for {
      tableId <- sendRequest("POST", "/tables", postTable) map { js =>
        {
          js.getLong("id")
        }
      }
      _ <- createDefaultColumns(tableId)
    } yield tableId
  }

  protected def createDefaultTable(
      name: String = "Test Table 1",
      tableNum: Int = 1,
      displayName: Option[JsonObject] = None,
      description: Option[JsonObject] = None
  ): Future[TableId] = {
    val fillStringCellJson = Json.obj("value" -> s"table${tableNum}row1")
    val fillStringCellJson2 = Json.obj("value" -> s"table${tableNum}row2")
    val fillNumberCellJson = Json.obj("value" -> 1)
    val fillNumberCellJson2 = Json.obj("value" -> 2)

    for {
      tableId <- createEmptyDefaultTable(name, tableNum, displayName, description)
      _ <- sendRequest("POST", s"/tables/$tableId/rows")
      _ <- sendRequest("POST", s"/tables/$tableId/rows")
      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/1", fillStringCellJson)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/2", fillStringCellJson2)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/1", fillNumberCellJson)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/2", fillNumberCellJson2)
    } yield tableId
  }

  protected def createFullTableWithMultilanguageColumns(
      tableName: String
  ): Future[(TableId, Seq[ColumnId], Seq[RowId])] = {

    def valuesRow(columnIds: Seq[Long]) = {
      Json.obj(
        "columns" -> Json.arr(
          Json.obj("id" -> columnIds.head),
          Json.obj("id" -> columnIds(1)),
          Json.obj("id" -> columnIds(2)),
          Json.obj("id" -> columnIds(3)),
          Json.obj("id" -> columnIds(4)),
          Json.obj("id" -> columnIds(5)),
          Json.obj("id" -> columnIds(6))
        ),
        "rows" -> Json.arr(
          Json.obj(
            "values" ->
              Json.arr(
                Json.obj(
                  "de-DE" -> s"Hallo, $tableName Welt!",
                  "en-GB" -> s"Hello, $tableName World!"
                ),
                Json.obj("de-DE" -> true),
                Json.obj("de-DE" -> 3.1415926),
                Json.obj("en-GB" -> s"Hello, $tableName Col 1 Row 1!"),
                Json.obj("en-GB" -> s"Hello, $tableName Col 2 Row 1!"),
                Json.obj("de-DE" -> "2015-01-01"),
                Json.obj("de-DE" -> "2015-01-01T14:37:47.110+01")
              )),
          Json.obj(
            "values" ->
              Json.arr(
                Json.obj(
                  "de-DE" -> s"Hallo, $tableName Welt2!",
                  "en-GB" -> s"Hello, $tableName World2!"
                ),
                Json.obj("de-DE" -> false),
                Json.obj("de-DE" -> 2.1415926),
                Json.obj("en-GB" -> s"Hello, $tableName Col 1 Row 2!"),
                Json.obj("en-GB" -> s"Hello, $tableName Col 2 Row 2!"),
                Json.obj("de-DE" -> "2015-01-02"),
                Json.obj("de-DE" -> "2015-01-02T14:37:47.110+01")
              ))
        )
      )
    }
    for {
      (tableId, columnIds) <- createTableWithMultilanguageColumns(tableName)
      rows <- sendRequest("POST", s"/tables/$tableId/rows", valuesRow(columnIds))
      _ = logger.info(s"Row is $rows")
      rowIds = rows.getJsonArray("rows").asScala.map(_.asInstanceOf[JsonObject].getLong("id").toLong).toSeq
    } yield (tableId, columnIds, rowIds)
  }

  protected def createSimpleTableWithMultilanguageColumn(
      tableName: String,
      columnName: String
  ): Future[(TableId, ColumnId)] = {
    for {
      table <- sendRequest("POST", "/tables", Json.obj("name" -> tableName))
      tableId = table.getLong("id").toLong
      columns <- sendRequest("POST",
                             s"/tables/$tableId/columns",
                             Json.obj(
                               "columns" -> Json.arr(
                                 Json.obj("kind" -> "text", "name" -> columnName, "languageType" -> "language")
                               )))
      columnId = columns.getJsonArray("columns").getJsonObject(0).getLong("id").toLong
    } yield {
      (tableId, columnId)
    }
  }

  protected def createTableWithMultilanguageColumns(tableName: String): Future[(TableId, Seq[ColumnId])] = {
    val createMultilanguageColumn = Json.obj(
      "columns" ->
        Json.arr(
          Json.obj("kind" -> "text", "name" -> "Test Column 1", "languageType" -> "language", "identifier" -> true),
          Json.obj("kind" -> "boolean", "name" -> "Test Column 2", "languageType" -> "language"),
          Json.obj("kind" -> "numeric", "name" -> "Test Column 3", "languageType" -> "language"),
          Json.obj("kind" -> "richtext", "name" -> "Test Column 4", "languageType" -> "language"),
          Json.obj("kind" -> "shorttext", "name" -> "Test Column 5", "languageType" -> "language"),
          Json.obj("kind" -> "date", "name" -> "Test Column 6", "languageType" -> "language"),
          Json.obj("kind" -> "datetime", "name" -> "Test Column 7", "languageType" -> "language")
        )
    )
    for {
      tableId <- sendRequest("POST", "/tables", Json.obj("name" -> tableName)) map (_.getLong("id"))
      columns <- sendRequest("POST", s"/tables/$tableId/columns", createMultilanguageColumn)
      columnIds = columns.getJsonArray("columns").asScala.map(_.asInstanceOf[JsonObject].getLong("id").toLong).toSeq
    } yield {
      (tableId.toLong, columnIds)
    }
  }

  protected def createTableWithComplexColumns(
      tableName: String,
      linkTo: TableId
  ): Future[(TableId, Seq[ColumnId], ColumnId)] = {
    val createColumns = Json.obj(
      "columns" -> Json.arr(
        Json.obj("kind" -> "text", "name" -> "column 1 (text)", "identifier" -> true),
        Json.obj("kind" -> "text", "name" -> "column 2 (text multilanguage)", "languageType" -> "language"),
        Json.obj("kind" -> "numeric", "name" -> "column 3 (numeric)"),
        Json.obj("kind" -> "numeric", "name" -> "column 4 (numeric multilanguage)", "languageType" -> "language"),
        Json.obj("kind" -> "richtext", "name" -> "column 5 (richtext)"),
        Json.obj("kind" -> "richtext", "name" -> "column 6 (richtext multilanguage)", "languageType" -> "language"),
        Json.obj("kind" -> "date", "name" -> "column 7 (date)"),
        Json.obj("kind" -> "date", "name" -> "column 8 (date multilanguage)", "languageType" -> "language"),
        Json.obj("kind" -> "attachment", "name" -> "column 9 (attachment)")
      )
    )

    def createLinkColumn(fromColumnId: ColumnId, linkTo: TableId) = {
      Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "kind" -> "link",
            "name" -> "column 10 (link)",
            "toTable" -> linkTo
          )))
    }
    for {
      table <- sendRequest("POST", "/tables", Json.obj("name" -> tableName))
      tableId = table.getLong("id").toLong

      columns <- sendRequest("POST", s"/tables/$tableId/columns", createColumns)
      columnIds = columns.getJsonArray("columns").asScala.map(_.asInstanceOf[JsonObject].getLong("id").toLong).toList

      linkColumn <- sendRequest("POST", s"/tables/$tableId/columns", createLinkColumn(columnIds.head, linkTo))
      linkColumnId = linkColumn.getJsonArray("columns").getJsonObject(0).getLong("id").toLong

    } yield (tableId, columnIds, linkColumnId)
  }

  protected def createSimpleTableWithCell(
      tableName: String,
      columnType: ColumnType
  ): Future[(TableId, ColumnId, RowId)] = {
    for {
      table <- sendRequest("POST", "/tables", Json.obj("name" -> tableName))
      tableId = table.getLong("id").toLong
      column <- sendRequest("POST", s"/tables/$tableId/columns", Json.obj("columns" -> Json.arr(columnType.getJson)))
      columnId = column.getJsonArray("columns").getJsonObject(0).getLong("id").toLong
      rowPost <- sendRequest("POST", s"/tables/$tableId/rows")
      rowId = rowPost.getLong("id").toLong
    } yield (tableId, columnId, rowId)
  }

  protected def createSimpleTableWithValues(
      tableName: String,
      columnTypes: Seq[ColumnType],
      rows: Seq[Seq[Any]]
  ): Future[(TableId, Seq[ColumnId], Seq[RowId])] = {
    for {
      table <- sendRequest("POST", "/tables", Json.obj("name" -> tableName))
      tableId = table.getLong("id").toLong
      column <- sendRequest("POST",
                            s"/tables/$tableId/columns",
                            Json.obj("columns" -> Json.arr(columnTypes.map(_.getJson): _*)))
      columnIds = column.getJsonArray("columns").asScala.toStream.map(_.asInstanceOf[JsonObject].getLong("id").toLong)
      columnsPost = Json.arr(columnIds.map(id => Json.obj("id" -> id)): _*)
      rowsPost = Json.arr(rows.map(values => Json.obj("values" -> Json.arr(values: _*))): _*)
      rowPost <- sendRequest("POST", s"/tables/$tableId/rows", Json.obj("columns" -> columnsPost, "rows" -> rowsPost))
      rowIds = rowPost.getJsonArray("rows").asScala.toStream.map(_.asInstanceOf[JsonObject].getLong("id").toLong)
    } yield (tableId, columnIds, rowIds)
  }
}
