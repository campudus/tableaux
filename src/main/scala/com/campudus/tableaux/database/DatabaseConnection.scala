package com.campudus.tableaux.database

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.{ Failure, Success, Try }
import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.VertxExecutionContext
import org.vertx.scala.core.eventbus.{ EventBus, Message }
import org.vertx.scala.core.json.{ Json, JsonArray, JsonObject }
import org.vertx.scala.platform.Verticle
import org.vertx.scala.core.AsyncResult

class DatabaseConnection(verticle: Verticle) {
  val DEFAULT_TIMEOUT = 5000L
  val address = "campudus.asyncdb"
  val eb = verticle.vertx.eventBus

  implicit val executionContext = VertxExecutionContext.fromVertxAccess(verticle)

  case class Transaction(msg: Message[JsonObject]) {

    def query(query: String, values: JsonArray): Future[(Transaction, JsonObject)] = transactionHelper(Json.obj(
      "action" -> "prepared",
      "statement" -> query,
      "values" -> values)) map { r => (Transaction(r), r.body()) }

    def commit(): Future[Unit] = transactionHelper(Json.obj("action" -> "commit")) map { _ => () }

    def rollback(): Future[Unit] = transactionHelper(Json.obj("action" -> "rollback")) map { _ => () }

    def recover(): PartialFunction[Throwable, Future[Transaction]] = {
      case ex: Throwable => rollback() flatMap (_ => Future.failed[Transaction](ex))
    }

    private def transactionHelper(jsonObj: JsonObject): Future[Message[JsonObject]] = {
      val p = Promise[Message[JsonObject]]()
      msg.replyWithTimeout(jsonObj, DEFAULT_TIMEOUT, {
        case Success(rep) => p.success(rep)
        case Failure(ex) =>
          verticle.logger.error(s"fail in ${jsonObj.getString("action")}", ex)
          p.failure(ex)
      }: Try[Message[JsonObject]] => Unit)
      p.future
    }
  }

  def begin(): Future[Transaction] = {
    val p = Promise[Transaction]()
    eb.sendWithTimeout(address, Json.obj("action" -> "begin"), DEFAULT_TIMEOUT, {
      case Success(rep) =>
        p.success(Transaction(rep))
      case Failure(ex) =>
        verticle.logger.error("fail in begin", ex)
        p.failure(ex)
    }: Try[Message[JsonObject]] => Unit)
    p.future
  }
}