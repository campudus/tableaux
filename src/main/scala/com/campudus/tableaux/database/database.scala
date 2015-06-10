package com.campudus.tableaux.database

import com.campudus.tableaux.database.domain.DomainObject
import com.campudus.tableaux.database.model.FolderModel._
import com.campudus.tableaux.{TableauxConfig, DatabaseException}
import com.campudus.tableaux.helper.StandardVerticle
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

trait DatabaseHelper {
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

    def query(stmt: String): Future[(Transaction, JsonObject)] = transactionHelper(Json.obj(
      "action" -> "raw",
      "command" -> stmt)) flatMap { r => Future.apply(Transaction(r), checkForDatabaseError(r.body())) recoverWith Transaction(r).rollbackAndFail() }

    def query(query: String, values: JsonArray): Future[(Transaction, JsonObject)] = transactionHelper(Json.obj(
      "action" -> "prepared",
      "statement" -> query,
      "values" -> values)) flatMap { r => Future.apply(Transaction(r), checkForDatabaseError(r.body())) recoverWith Transaction(r).rollbackAndFail() }

    def commit(): Future[Unit] = transactionHelper(Json.obj("action" -> "commit")) map { _ => () }

    def rollback(): Future[Unit] = transactionHelper(Json.obj("action" -> "rollback")) map { _ => () }

    def rollbackAndFail(): PartialFunction[Throwable, Future[(Transaction, JsonObject)]] = {
      case ex: Throwable => rollback() flatMap (_ => Future.failed[(Transaction, JsonObject)](ex))
    }

    private def transactionHelper(json: JsonObject): Future[Message[JsonObject]] = {
      val p = Promise[Message[JsonObject]]()
      msg.replyWithTimeout(json, DEFAULT_TIMEOUT, replyHandler(p, json.getString("action")))
      p.future
    }
  }

  def query(stmt: String): Future[JsonObject] = sendHelper(Json.obj(
    "action" -> "raw",
    "command" -> stmt
  )) map { msg => checkForDatabaseError(msg.body()) } recoverWith {case ex => Future.failed[JsonObject](ex)}

  def query(stmt: String, parameter: JsonArray): Future[JsonObject] = sendHelper(Json.obj(
    "action" -> "prepared",
    "statement" -> stmt,
    "values" -> parameter)) map { msg => checkForDatabaseError(msg.body()) } recoverWith { case ex => Future.failed[JsonObject](ex) }

  def begin(): Future[Transaction] = sendHelper(Json.obj("action" -> "begin")) map { Transaction }

  def transactional[A](stuff: TransFunc[A]): Future[A] = {
    for {
      transaction <- begin()
      (transaction, result) <- stuff(transaction)
      _ <- transaction.commit()
    } yield result
  }

  private def sendHelper(json: JsonObject): Future[Message[JsonObject]] = {
    val p = Promise[Message[JsonObject]]()
    vertx.eventBus.sendWithTimeout(config.databaseAddress, json, DEFAULT_TIMEOUT, replyHandler(p, json.getString("action")))
    p.future
  }

  private def replyHandler(p: Promise[Message[JsonObject]], action: String): Try[Message[JsonObject]] => Unit = {
    case Success(rep) => p.success(rep)
    case Failure(ex) =>
      verticle.logger.error(s"fail in $action", ex)
      p.failure(ex)
  }

  private def checkForDatabaseError(json: JsonObject): JsonObject = json.getString("status") match {
    case "ok" => json
    case "error" => throw DatabaseException(json.getString("message"), "unknown")
  }
}