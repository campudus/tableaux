package com.campudus.tableaux.verticles

import com.campudus.tableaux.verticles.EventClient._

import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.eventbus.Message
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

import com.typesafe.scalalogging.LazyLogging
import org.everit.json.schema.Schema
import org.everit.json.schema.ValidationException
import org.everit.json.schema.loader.SchemaLoader
import org.json.{JSONArray, JSONObject};

object ValidatorKeys {
  val ATTRIBUTES = "attributes"
  val STATUS = "status"
}

class JsonSchemaValidatorVerticle extends ScalaVerticle with LazyLogging {

  private lazy val eventBus = vertx.eventBus()
  private var validators: Map[String, Schema] = Map()

  private def createSchemaValidator(schemaString: String): Schema = {
    SchemaLoader.load(new JSONObject(schemaString))
  }

  override def startFuture(): Future[_] = {
    eventBus.consumer(ADDRESS_JSON_SCHEMA_VALIDATE, messageHandlerValidateJson).completionFuture()
    eventBus.consumer(ADDRESS_JSON_SCHEMA_REGISTER, messageHandlerRegisterSchema).completionFuture()
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
    val key = keyWithJson.getString("key")
    val jsonType = keyWithJson.getString("jsonType")
    val jsonToValidate = jsonType match {
      case "array" => new JSONArray(keyWithJson.getJsonArray("jsonToValidate").encode())
      case _ => new JSONObject(keyWithJson.getJsonObject("jsonToValidate").encode())
    }
    val validatorOption = validators.get(key)
    validatorOption match {
      case Some(validator) => {
        Try(validator.validate(jsonToValidate)) match {
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
