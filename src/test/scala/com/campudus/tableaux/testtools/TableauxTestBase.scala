package com.campudus.tableaux.testtools

import com.campudus.tableaux.{CustomException, Starter, TableauxConfig}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.DomainObject
import com.campudus.tableaux.database.model.SystemModel
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.helper.FileUtils
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}
import com.campudus.tableaux.testtools.RequestCreation.ColumnType

import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.{ScalaVerticle, VertxExecutionContext}
import io.vertx.scala.FutureHelper._
import io.vertx.scala.SQLConnection
import io.vertx.scala.core.{DeploymentOptions, Vertx}
import io.vertx.scala.core.file.{AsyncFile, OpenOptions}
import io.vertx.scala.core.http._
import io.vertx.scala.core.streams.Pump
import org.vertx.scala.core.json.{JsonObject, _}

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

import com.typesafe.scalalogging.LazyLogging
import org.junit.{After, Before}
import org.junit.runner.RunWith

case class TestCustomException(message: String, id: String, statusCode: Int) extends Throwable {

  override def toString: String = s"TestCustomException(status=$statusCode,id=$id,message=$message)"
}

case class TestTableauxUser() {
  TableauxUser("Test", Seq.empty[String])
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

  override var cdnConfig: JsonObject = _

  override var thumbnailsConfig: JsonObject = _

  override var host: String = _

  override var port: Int = _

  override var tableauxConfig: TableauxConfig = _

  // default access token used for all integration tests if no explicit token is provided
  var wildcardAccessToken: String = _

  implicit var user: TableauxUser = _

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
    cdnConfig = config.getJsonObject("cdn", Json.obj())
    thumbnailsConfig = config.getJsonObject("thumbnails", Json.obj())

    val rolePermissionsPath = config.getString("rolePermissionsPath")
    val rolePermissions = FileUtils(this.vertxAccess()).readJsonFile(rolePermissionsPath, Json.emptyObj())

    host = config.getString("host")
    port = config.getInteger("port").intValue()

    tableauxConfig = new TableauxConfig(
      vertx,
      authConfig,
      databaseConfig,
      cdnConfig,
      thumbnailsConfig,
      config.getString("workingDirectory"),
      config.getString("uploadsDirectory"),
      rolePermissions,
      isRowPermissionCheckEnabled = false
    )

    val async = context.async()

    val options = DeploymentOptions()
      .setConfig(config)

    val completionHandler = {
      case Success(id) =>
        logger.info(s"Verticle deployed with ID $id")
        async.complete()

      case Failure(e) =>
        logger.error("Verticle couldn't be deployed.", e)
        context.fail(e)
        async.complete()
    }: Try[String] => Unit

    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val system = SystemModel(dbConnection)
    for {
      _ <- system.uninstall()
      _ <- system.installShortCutFunction()
      _ <- system.install()
    } yield {
      vertx
        .deployVerticleFuture(ScalaVerticle.nameForVerticle[Starter], options)
    }.onComplete(completionHandler)

    val tokenHelper = TokenHelper(this.vertxAccess())

    wildcardAccessToken = tokenHelper.generateToken(
      Json.obj(
        "aud" -> "grud-backend",
        "iss" -> "campudus-test",
        "preferred_username" -> "Test",
        "realm_access" -> Json.obj("roles" -> Json.arr("dev"))
      )
    )

    user = TableauxUser("", Seq.empty[String])
  }

  @After
  def after(context: TestContext): Unit = vertx.close(context.asyncAssertSuccess())

  /**
    * Initializes the RoleModel with the given config and also sets up the requestsContext with all provided roles
    */
  def initRoleModel(roleConfig: String): RoleModel = {
    val roleModel: RoleModel = RoleModel(Json.fromObjectString(roleConfig.stripMargin))

    val roles = collection.immutable.Seq(roleModel.role2permissions.keySet.toSeq: _*)

    setRequestRoles(roles)

    roleModel
  }

  protected def setRequestRoles(roles: Seq[String]): Unit = {
    user = TableauxUser("Test", roles)
  }

 // format: off
  /**
    * Helper method to set up Tests without running into permission problem (UnauthorizedException)
    *
    * 1. Sets a userRole for requestContext
    * 2. Invokes a function block with this userRole
    * 3. Resets the original userRoles defined by the test
    *
    * For this purpose there is a dummy test role "dev" in `role-permissions-test.json`.
    */
  protected def asDevUser[A](function: => Future[A]): Future[A] = {
    // format: on
    val userRolesFromTest: Seq[String] = collection.immutable.Seq(user.roles: _*)

    setRequestRoles(Seq("dev"))

    val result: Future[A] = function

    setRequestRoles(userRolesFromTest)

    result
  }

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

  /**
    * Extension method for Future to simplify exception handling in tests. Recovers from any exception and returns it as
    * a successful value. Useful for testing error cases without breaking the for-comprehension.
    *
    * Example usage:
    * {{{
    *   val ex <- controller.createCompleteTable(...).toException()
    * }}}
    * Instead of:
    * {{{
    *   val ex <- controller.createCompleteTable(...).recover({ case ex => ex })
    * }}}
    */
  implicit class FutureRecoverExtensions[A](future: Future[A]) {
    def toException(): Future[Any] = future.recover({ case ex => ex })
  }

  def sendRequest(method: String, path: String): Future[JsonObject] = {
    sendRequest(method, path, None)
  }

  def sendRequest(method: String, path: String, tokenOpt: Option[String]): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p, tokenOpt).end()
    p.future
  }

  def sendRequest(method: String, path: String, jsonObj: JsonObject): Future[JsonObject] = {
    sendRequest(method, path, jsonObj, None)
  }

  def sendRequest(method: String, path: String, jsonObj: JsonObject, tokenOpt: Option[String]): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p, tokenOpt).end(jsonObj.encode())
    p.future
  }

  def sendRequest(method: String, path: String, body: String): Future[JsonObject] = {
    sendRequest(method, path, body, None)
  }

  def sendRequest(method: String, path: String, body: String, tokenOpt: Option[String]): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p, tokenOpt).end(body)
    p.future
  }

  def sendRequest(method: String, path: String, domainObject: DomainObject): Future[JsonObject] = {
    sendRequest(method, path, domainObject.getJson)
  }

  def sendStringRequest(method: String, path: String): Future[String] = {
    val p = Promise[String]()
    httpStringRequest(method, path, p, None).end()
    p.future
  }

  def sendStringRequest(method: String, path: String, jsonObj: JsonObject): Future[String] = {
    val p = Promise[String]()
    httpStringRequest(method, path, p, None).end(jsonObj.encode())
    p.future
  }

  private def createResponseHandler[A](
      p: Promise[A],
      function: String => A
  ): (HttpClient, HttpClientResponse) => Unit = { (client: HttpClient, resp: HttpClientResponse) =>
    def bodyHandler = new Handler[Buffer] {
      override def handle(buf: Buffer): Unit = {
        val body = buf.toString()

        client.close()

        if (resp.statusCode() != 200) {
          p.failure(TestCustomException(body, resp.statusMessage(), resp.statusCode()))
        } else {
          try {
            p.success(function(body))
          } catch {
            case ex: Exception => p.failure(ex)
          }
        }
      }
    }

    resp.bodyHandler(bodyHandler)
  }

  private def createExceptionHandler[A](p: Promise[A]): (HttpClient, Throwable) => Unit = {
    (client: HttpClient, x: Throwable) =>
      client.close()
      p.failure(x)
  }

  private def httpStringRequest(
      method: String,
      path: String,
      p: Promise[String],
      tokenOpt: Option[String]
  ): HttpClientRequest = {
    httpRequest(method, path, createResponseHandler[String](p, _.toString), createExceptionHandler[String](p), tokenOpt)
  }

  private def httpJsonRequest(
      method: String,
      path: String,
      p: Promise[JsonObject],
      tokenOpt: Option[String]
  ): HttpClientRequest = {
    httpRequest(
      method,
      path,
      createResponseHandler[JsonObject](p, Json.fromObjectString),
      createExceptionHandler[JsonObject](p),
      tokenOpt
    )
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
      .handler(new Handler[HttpClientResponse] {
        override def handle(resp: HttpClientResponse): Unit = responseHandler(client, resp)
      })
      .exceptionHandler(new Handler[Throwable] {
        override def handle(x: Throwable): Unit = exceptionHandler(client, x)
      })
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

          file.exceptionHandler(new Handler[Throwable] {
            override def handle(e: Throwable): Unit = {
              pump.stop()
              req.end("")
              p.failure(e)
            }
          })

          file.endHandler(new Handler[Unit] {
            override def handle(event: Unit): Unit = {
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
            }
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
      columnId2 = column2.getJsonArray("columns").getJsonObject(0).getLong("id").toLong
    } yield (columnId1, columnId2)
  }

  protected def createStatusTestColumns(tableId: TableId): Future[Unit] = {
    val createShortTextColumnJson =
      Json.obj("columns" -> Json.arr(Json.obj("kind" -> "shorttext", "name" -> "Test Column 1")))
    val createRichTextColumnJson =
      Json.obj("columns" -> Json.arr(Json.obj("kind" -> "richtext", "name" -> "Test Column 2")))
    val createNumberColumnJson =
      Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 3")))

    val createBooleanColumnJson =
      Json.obj("columns" -> Json.arr(Json.obj("kind" -> "boolean", "name" -> "Test Column 4")))

    for {
      _ <- sendRequest("POST", s"/tables/$tableId/columns", createShortTextColumnJson)
      _ <- sendRequest("POST", s"/tables/$tableId/columns", createRichTextColumnJson)
      _ <- sendRequest("POST", s"/tables/$tableId/columns", createNumberColumnJson)
      _ <- sendRequest("POST", s"/tables/$tableId/columns", createBooleanColumnJson)
    } yield ()
  }

  protected def createFullStatusTestTable(): Future[TableId] = {
    val postTable = Json.obj("name" -> "status test table")
    val shortTextValue = Json.obj("value" -> "short_text_value")
    val richTextValue = Json.obj("value" -> "rich_text_value")
    val numberValue = Json.obj("value" -> 42)
    val booleanValue = Json.obj("value" -> true)
    for {
      tableId <- sendRequest("POST", "/tables", postTable) map { js =>
        {
          js.getLong("id")
        }
      }
      _ <- createStatusTestColumns(tableId)
      _ <- sendRequest("POST", s"/tables/$tableId/rows")
      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/1", shortTextValue)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/1", richTextValue)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/3/rows/1", numberValue)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/4/rows/1", booleanValue)
    } yield tableId
  }

  protected def createEmptyDefaultTable(
      name: String = "Test Table 1",
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
      tableId <- createEmptyDefaultTable(name, displayName, description)
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
              )
          ),
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
              )
          )
        )
      )
    }
    for {
      (tableId, columnIds) <- createTableWithMultilanguageColumns(tableName)
      rows <- sendRequest("POST", s"/tables/$tableId/rows", valuesRow(columnIds))
      _ = logger.info(s"Row is $rows")
      rowIds = rows.getJsonArray("rows").asScala.map(_.asInstanceOf[JsonObject].getLong("id").toLong).toSeq
    } yield (tableId, columnIds, collection.immutable.Seq(rowIds: _*))
  }

  protected def createSimpleTableWithMultilanguageColumn(
      tableName: String,
      columnName: String
  ): Future[(TableId, ColumnId)] = {
    for {
      table <- sendRequest("POST", "/tables", Json.obj("name" -> tableName))
      tableId = table.getLong("id").toLong
      columns <- sendRequest(
        "POST",
        s"/tables/$tableId/columns",
        Json.obj(
          "columns" -> Json.arr(
            Json.obj("kind" -> "text", "name" -> columnName, "languageType" -> "language")
          )
        )
      )
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
      (tableId.toLong, collection.immutable.Seq(columnIds: _*))
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
          )
        )
      )
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
      column <- sendRequest(
        "POST",
        s"/tables/$tableId/columns",
        Json.obj("columns" -> Json.arr(columnTypes.map(_.getJson): _*))
      )
      columnIds = column.getJsonArray("columns").asScala.toStream.map(_.asInstanceOf[JsonObject].getLong("id").toLong)
      columnsPost = Json.arr(columnIds.map(id => Json.obj("id" -> id)): _*)
      rowsPost = Json.arr(rows.map(values => Json.obj("values" -> Json.arr(values: _*))): _*)
      rowPost <- sendRequest("POST", s"/tables/$tableId/rows", Json.obj("columns" -> columnsPost, "rows" -> rowsPost))
      rowIds = rowPost.getJsonArray("rows").asScala.toStream.map(_.asInstanceOf[JsonObject].getLong("id").toLong)
    } yield (tableId, columnIds, rowIds)
  }
}
