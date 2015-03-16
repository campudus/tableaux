package com.campudus.tableaux.database

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.{ Failure, Success, Try }
import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.VertxExecutionContext
import org.vertx.scala.core.eventbus.{ EventBus, Message }
import org.vertx.scala.core.json.{ Json, JsonArray, JsonObject }
import org.vertx.scala.platform.Verticle
import com.campudus.tableaux.DatabaseException

class DatabaseConnection(verticle: Verticle) {
  val DEFAULT_TIMEOUT = 5000L
  val address = "campudus.asyncdb"
  val eb = verticle.vertx.eventBus

  implicit val executionContext = VertxExecutionContext.fromVertxAccess(verticle)

  case class Transaction(msg: Message[JsonObject]) {

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

  def singleQuery(query: String, values: JsonArray): Future[JsonObject] = sendHelper(Json.obj(
    "action" -> "prepared",
    "statement" -> query,
    "values" -> values)) map { msg => checkForDatabaseError(msg.body()) } recoverWith { case ex => Future.failed[JsonObject](ex) }

  def begin(): Future[Transaction] = sendHelper(Json.obj("action" -> "begin")) map { Transaction(_) }

  private def sendHelper(json: JsonObject): Future[Message[JsonObject]] = {
    val p = Promise[Message[JsonObject]]()
    eb.sendWithTimeout(address, json, DEFAULT_TIMEOUT, replyHandler(p, json.getString("action")))
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