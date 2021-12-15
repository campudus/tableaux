package com.campudus.tableaux.verticles.JsonSchemaValidator
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.eventbus.Message
import scala.concurrent.Future
import org.vertx.scala.core.json.{Json, JsonObject}
import com.typesafe.scalalogging.LazyLogging
import scala.language.implicitConversions
import org.json.JSONObject;
import scala.util.{Failure, Success, Try}
import org.everit.json.schema.ValidationException;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

class JsonSchemaValidatorVerticle extends ScalaVerticle with LazyLogging {

  private lazy val eventBus = vertx.eventBus()
  private var validators: Map[String, Schema] = Map()

  private def createSchemaValidator(schemaString: String): Schema = {
    SchemaLoader.load(new JSONObject(schemaString))
  }

  override def startFuture(): Future[_] = {
    eventBus.consumer("json.schema.validate", messageHandlerValidateJson).completionFuture()
    eventBus.consumer("json.schema.register", messageHandlerRegisterSchema).completionFuture()
  }

  private def messageHandlerRegisterSchema(message: Message[JsonObject]): Unit = {
    val keyWithSchema: JsonObject = message.body()
    val key = keyWithSchema.getString("key")
    val schemaString = keyWithSchema.getJsonObject("schema")

    validators += (key -> createSchemaValidator(schemaString.encode()))
    message.reply("")
  }

  private def messageHandlerValidateJson(message: Message[JsonObject]): Unit = {
    val keyWithJson = message.body()
    val jsonToValidate = keyWithJson.getJsonObject("jsonToValidate")
    val key = keyWithJson.getString("key")
    val attributes = new JSONObject(jsonToValidate.encode())
    val validatorOption = validators.get(key)
    validatorOption match {
      case Some(validator) => {
        Try(validator.validate(attributes)) match {
          case Success(v) => {
            message.reply("")
          }
          case Failure(e) => {
            message.fail(400, e.getMessage())
          }
        }

      }
      case None => message.fail(400, s"Schema with key $key unknown")
    }

  }
}
