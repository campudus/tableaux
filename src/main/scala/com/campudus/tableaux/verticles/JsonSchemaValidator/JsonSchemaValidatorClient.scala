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

  def validateJson(key: String, json: JsonObject): Future[Unit] = {
    val containerJson = Json.obj("key" -> key, "jsonToValidate" -> json, "jsonType" -> "object")
    eventBus.sendFuture[String]("json.schema.validate", containerJson).map((f: Message[String]) => {})
  }

  def validateJson(key: String, json: JsonArray): Future[Unit] = {
    val containerJson = Json.obj("key" -> key, "jsonToValidate" -> json, "jsonType" -> "array")
    eventBus.sendFuture[String]("json.schema.validate", containerJson).map((f: Message[String]) => {})
  }

  def registerMultipleSchemas(schemaWithKeyList: List[JsonObject]): Future[List[Unit]] = {
    Future.sequence(schemaWithKeyList.map(registerSchema))
  }

  def registerSchema(schemaWithKey: JsonObject): Future[Unit] = {
    eventBus.sendFuture[String]("json.schema.register", schemaWithKey).map((f: Message[String]) => {})
  }
}

object ValidatorKeys {
  val ATTRIBUTES = "attributes"
  val STATUS = "status"
}
