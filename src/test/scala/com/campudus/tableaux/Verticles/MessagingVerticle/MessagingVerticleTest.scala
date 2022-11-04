package com.campudus.tableaux.Verticles.MessagingVerticle

import com.campudus.tableaux.{CustomException, Starter, TableauxConfig}
import com.campudus.tableaux.controller.StructureController
import com.campudus.tableaux.controller.SystemController
import com.campudus.tableaux.database.{DatabaseConnection, LanguageNeutral, TextType}
import com.campudus.tableaux.database.domain.{CreateSimpleColumn, GenericTable}
import com.campudus.tableaux.database.domain.MultiLanguageValue
import com.campudus.tableaux.database.domain.Service
import com.campudus.tableaux.database.domain.ServiceType
import com.campudus.tableaux.database.model.ServiceModel
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.model.SystemModel
import com.campudus.tableaux.database.model.TableauxModel
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.helper.FileUtils
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.campudus.tableaux.testtools.TableauxTestBase
import com.campudus.tableaux.testtools.TokenHelper
import com.campudus.tableaux.verticles.Messaging.MessagingVerticle
import com.campudus.tableaux.verticles.MessagingVerticle.MessagingVerticleClient

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.{ScalaVerticle, VertxExecutionContext}
import io.vertx.scala.SQLConnection
import io.vertx.scala.core.{DeploymentOptions, Vertx}
import org.vertx.scala.core.json.{JsonObject, _}

import scala.collection.mutable.MutableList
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import org.junit.{After, Before}
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.spy
import org.mockito.Mockito.when
import org.mockito.MockitoAnnotations
import org.mockito.captor.ArgCaptor
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatestplus.mockito.MockitoSugar

@RunWith(classOf[VertxUnitRunner])
class MessagingVerticleTest extends TableauxTestBase with MockitoSugar {
  val answers: MutableList[JsonObject] = MutableList()
  var messagingClient: MessagingVerticleClient = _

  def addToAnswers(obj: JsonObject): Unit = {
    answers += obj
  }

  @Before
  override def before(context: TestContext): Unit = {
    vertx = Vertx.vertx()

    executionContext = VertxExecutionContext(
      io.vertx.scala.core.Context(vertx.asJava.asInstanceOf[io.vertx.core.Vertx].getOrCreateContext())
    )

    messagingClient = MessagingVerticleClient(vertx)

    val config = Json
      .fromObjectString(fileConfig.encode())
      .put("host", fileConfig.getString("host", "127.0.0.1"))
      .put("port", getFreePort)

    databaseConfig = config.getJsonObject("database", Json.obj())
    authConfig = config.getJsonObject("auth", Json.obj())

    val rolePermissionsPath = config.getString("rolePermissionsPath")
    val rolePermissions = FileUtils(this.vertxAccess()).readJsonFile(rolePermissionsPath, Json.emptyObj())

    host = config.getString("host")
    port = config.getInteger("port").intValue()

    tableauxConfig = new TableauxConfig(
      vertx,
      authConfig,
      databaseConfig,
      config.getString("workingDirectory"),
      config.getString("uploadsDirectory"),
      rolePermissions
    )

    val async = context.async()

    val completionHandler = {
      case Success(id) =>
        logger.info(s"Verticle deployed with ID $id")
        async.complete()

      case Failure(e) =>
        logger.error("Verticle couldn't be deployed.", e)
        context.fail(e)
        async.complete()
    }: Try[String] => Unit

    val verticleConfig =
      Json.obj(
        "rolePermissions" -> tableauxConfig.rolePermissions,
        "authConfig" -> tableauxConfig.authConfig,
        "databaseConfig" -> tableauxConfig.databaseConfig
      )
    val options = DeploymentOptions()
      .setConfig(verticleConfig)

    val spiedMessagingVerticle = spy(new MessagingVerticle)
    var listenersCaptor = ArgCaptor[Seq[Service]]
    var payloadCaptor = ArgCaptor[JsonObject]
    doAnswer(new Answer[Future[Seq[Any]]] {
      override def answer(i: InvocationOnMock): Future[Seq[Any]] = {
        val listeners = i.getArgument[Seq[Service]](0)
        val payload = i.getArgument[JsonObject](1)
        val futures = listeners.map(listener => {
          val name = listener.name
          val config = listener.config
          val res =
            Json.obj(
              "listenerName" -> name,
              "payload" -> payload
            )
          addToAnswers(res)
          Future(
            res
          )
        })
        Future.sequence(futures)

      }
    }).when(spiedMessagingVerticle).sendMessage(
      listenersCaptor,
      payloadCaptor
    )
    implicit val roleModel = RoleModel(tableauxConfig.rolePermissions)

    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val system = SystemModel(dbConnection)
    logger.info("before deploy")
    for {
      _ <- system.uninstall()
      _ <- system.install()
    } yield {
      vertx
        .deployVerticleFuture(spiedMessagingVerticle, options)
    }.onComplete(completionHandler)
    logger.info("after deploy")

    val tokenHelper = TokenHelper(this.vertxAccess())

    wildcardAccessToken = tokenHelper.generateToken(
      Json.obj(
        "aud" -> "grud-backend",
        "iss" -> "campudus-test",
        "preferred_username" -> "Test",
        "realm_access" -> Json.obj("roles" -> Json.arr("dev"))
      )
    )

    user = TableauxUser("", Seq("dev"))
    logger.info("before finished")
  }

  @After
  override def after(context: TestContext): Unit = {
    answers.drop(answers.size)
    vertx.close(context.asyncAssertSuccess())
  }

  def createStructureModel(): StructureModel = {
    logger.info("createStrucutreController")
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    implicit val roleModel = RoleModel(tableauxConfig.rolePermissions)
    val model = StructureModel(dbConnection)

    // StructureController(tableauxConfig, model, roleModel)
    model
  }

  def createSystemController(): SystemController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    implicit val roleModel = RoleModel(tableauxConfig.rolePermissions)
    val structureModel = StructureModel(dbConnection)
    val systemModel = SystemModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel)
    val serviceModel = ServiceModel(dbConnection)

    SystemController(tableauxConfig, systemModel, tableauxModel, structureModel, serviceModel, roleModel)
  }

  def createScope(
      tableIncludes: Seq[JsonObject] = Seq(Json.obj("name" -> ".*")),
      columnIncludes: Seq[JsonObject] = Seq(Json.obj("name" -> ".*")),
      tableExcludes: Seq[JsonObject] = Seq(),
      columnExcludes: Seq[JsonObject] = Seq()
  ): JsonObject = {
    Json.obj(
      "scope" -> "global",
      "tables" -> Json.obj(
        "includes" -> tableIncludes,
        "excludes" -> tableExcludes
      ),
      "columns" -> Json.obj(
        "includes" -> columnIncludes,
        "excludes" -> columnExcludes
      )
    )
  }

  def createListenerConfig(
      events: Seq[String],
      host: String = "test_host",
      port: Int = 666,
      route: String = "/",
      headers: JsonObject = Json.obj()
  ): JsonObject = {
    Json.obj(
      "events" -> events,
      "host" -> host,
      "port" -> port,
      "route" -> route,
      "headers" -> headers
    )
  }

  def createListener(name: String, config: JsonObject, scope: JsonObject = createScope()): Future[Service] = {
    val systemController = createSystemController()
    for {
      service <- systemController.createService(
        name,
        ServiceType(Some("listener")),
        None,
        MultiLanguageValue(),
        MultiLanguageValue(),
        true,
        Some(config),
        Some(scope)
      ).map(obj => obj.asInstanceOf[Service])
      _ <- messagingClient.servicesChange()
    } yield service
  }

  def createExpectedJson(
      name: String,
      event: String,
      data: JsonObject,
      tableId: Option[TableId],
      columnId: Option[ColumnId] = None,
      rowId: Option[RowId] = None
  ): JsonObject = {
    val payload = Json.obj(
      "event" -> event,
      "data" -> data
    )
    tableId match {
      case Some(id) => payload.put("tableId", id)
      case _ => {}
    }
    columnId match {
      case Some(id) => payload.put("columnId", id)
      case _ => {}
    }
    rowId match {
      case Some(id) => payload.put("rowId", id)
      case _ => {}
    }
    Json.obj(
      "listenerName" -> name,
      "payload" -> payload
    )
  }

  @Test
  def createTableTest(implicit c: TestContext): Unit = okTest {
    logger.info("createTableTest")
    val structureModel = createStructureModel()
    val listenerName = "table_creation_listener"
    val event = MessagingVerticle.EVENT_TYPE_TABLE_CREATE
    val listenerConfig = createListenerConfig(Seq(event))

    for {
      _ <- createListener(listenerName, listenerConfig)
      table <- structureModel.tableStruc.create(
        "test_table_1",
        hidden = false,
        langtags = None,
        displayInfos = Seq(),
        tableType = GenericTable,
        tableGroupIdOpt = None,
        attributes = None
      )
      res <- messagingClient.tableCreated(table.id)
    } yield {
      val expected = createExpectedJson(listenerName, event, table.getJson, Some(table.id))
      assertJSONEquals(expected, answers.head)
    }

  }
}
