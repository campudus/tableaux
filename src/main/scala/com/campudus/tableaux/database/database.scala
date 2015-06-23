package com.campudus.tableaux.database

import com.campudus.tableaux.database.domain.DomainObject
import com.campudus.tableaux.database.model.FolderModel._
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.helper.StandardVerticle
import com.campudus.tableaux.{DatabaseException, TableauxConfig}
import org.joda.time.DateTime
import org.vertx.scala.core.eventbus.Message
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}
import org.vertx.scala.platform.Verticle
import org.vertx.scala.core.FunctionConverters._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

trait DatabaseQuery {
  protected[this] val connection: DatabaseConnection

  implicit val executionContext = connection.executionContext
}

sealed trait DatabaseHelper {
  implicit def convertLongToFolderId(id: Long): Option[FolderId] = {
    //TODO still, not cool!
    Option(id).filter(_ != 0)
  }

  implicit def convertStringToDateTime(str: String): Option[DateTime] = {
    Option(str).map(DateTime.parse)
  }
}

trait DatabaseHandler[O <: DomainObject, ID] extends DatabaseQuery with DatabaseHelper {
  def add(o: O): Future[O]

  def retrieve(id: ID): Future[O]

  def retrieveAll(): Future[Seq[O]]

  def update(o: O): Future[O]

  def delete(o: O): Future[Unit]

  def deleteById(id: ID): Future[Unit]

  def size(): Future[Long]
}

object DatabaseConnection {
  val DEFAULT_TIMEOUT = 5000L

  def apply(config: TableauxConfig): DatabaseConnection = {
    new DatabaseConnection(config)
  }
}

class DatabaseConnection(val config: TableauxConfig) extends StandardVerticle {

  import DatabaseConnection._

  override val verticle: Verticle = config.verticle

  type TransFunc[+A] = Transaction => Future[(Transaction, A)]

  case class Transaction(msg: Message[JsonObject]) {

    def query(stmt: String): Future[(Transaction, JsonObject)] = {
      val command = Json.obj(
        "action" -> "raw",
        "command" -> stmt
      )

      queryHelper(command)
    }

    def query(stmt: String, values: JsonArray): Future[(Transaction, JsonObject)] = {
      val command = Json.obj(
        "action" -> "prepared",
        "statement" -> stmt,
        "values" -> values
      )

      queryHelper(command)
    }

    def commit(): Future[Unit] = transactionHelper(Json.obj("action" -> "commit")) map { _ => () }

    def rollback(): Future[Unit] = transactionHelper(Json.obj("action" -> "rollback")) map { _ => () }

    def rollbackAndFail(): PartialFunction[Throwable, Future[(Transaction, JsonObject)]] = {
      case ex: Throwable => rollback() flatMap (_ => Future.failed[(Transaction, JsonObject)](ex))
    }

    private def queryHelper(command: JsonObject): Future[(Transaction, JsonObject)] = {
      transactionHelper(command) flatMap { reply =>
        val future = Future(Transaction(reply), checkForDatabaseError(command, reply.body()))
        future recoverWith Transaction(reply).rollbackAndFail()
        future
      }
    }

    private def transactionHelper(json: JsonObject): Future[Message[JsonObject]] = {
      val p = Promise[Message[JsonObject]]()
      msg.replyWithTimeout(json, DEFAULT_TIMEOUT, replyHandler(p, json))
      p.future
    }
  }

  def query(stmt: String): Future[JsonObject] = {
    val command = Json.obj(
      "action" -> "raw",
      "command" -> stmt
    )
    queryHelper(command)
  }

  def query(stmt: String, parameter: JsonArray): Future[JsonObject] = {
    val command = Json.obj(
      "action" -> "prepared",
      "statement" -> stmt,
      "values" -> parameter
    )
    queryHelper(command)
  }

  def begin(): Future[Transaction] = sendHelper(Json.obj("action" -> "begin")) map Transaction

  def transactional[A](stuff: TransFunc[A]): Future[A] = {
    for {
      transaction <- begin()
      (transaction, result) <- stuff(transaction)
      _ <- transaction.commit()
    } yield result
  }

  def selectSingleLong(select: String, arr: JsonArray): Future[Long] = {
    for {
      result <- query(select, arr)
      resultArr <- Future(selectNotNull(result))
    } yield {
      resultArr.head.get[Long](0)
    }
  }

  private def queryHelper(command: JsonObject): Future[JsonObject] = {
    sendHelper(command) map { reply => checkForDatabaseError(command, reply.body()) } recoverWith { case ex => Future.failed[JsonObject](ex) }
  }

  private def sendHelper(json: JsonObject): Future[Message[JsonObject]] = {
    val p = Promise[Message[JsonObject]]()
    vertx.eventBus.sendWithTimeout(config.databaseAddress, json, DEFAULT_TIMEOUT, replyHandler(p, json))
    p.future
  }

  private def replyHandler(p: Promise[Message[JsonObject]], json: JsonObject): Try[Message[JsonObject]] => Unit = {
    case Success(rep) => p.success(rep)
    case Failure(ex) =>
      verticle.logger.error(s"fail in ${json.getString("action")}: ${json.encode()}", ex)
      p.failure(ex)
  }

  private def checkForDatabaseError(command: JsonObject, reply: JsonObject): JsonObject = reply.getString("status") match {
    case "ok" => reply
    case "error" => throw DatabaseException(s"Statement ${command.encode()} failed. ${reply.getString("message")}", "unknown")
  }
}