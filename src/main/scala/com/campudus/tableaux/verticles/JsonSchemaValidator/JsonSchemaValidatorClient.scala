package com.campudus.tableaux.verticles.JsonSchemaValidator
import com.campudus.tableaux.helper.VertxAccess
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus._
import org.vertx.scala.core.json._
import scala.concurrent.Future
import io.vertx.scala.core.eventbus.Message
import scala.util.{Success, Failure}

object JsonSchemaValidatorClient {

  def apply(vertx: Vertx): JsonSchemaValidatorClient = {
    new JsonSchemaValidatorClient(vertx)
  }
}

class JsonSchemaValidatorClient(vertxAccess: Vertx) extends VertxAccess {
  val vertx = vertxAccess
  val eventBus = vertx.eventBus()

  def validateAttributesJson(json: String): Future[Unit] = {
    eventBus.sendFuture[String]("json.schema.validate.attributes", json).map((f: Message[String]) => {})
  }
}
