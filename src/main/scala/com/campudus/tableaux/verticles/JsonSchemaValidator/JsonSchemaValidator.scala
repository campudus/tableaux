package com.campudus.tableaux.verticles.JsonSchemaValidator
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.eventbus.Message
import scala.concurrent.Future
import org.vertx.scala.core.json.{Json, JsonObject}
import com.typesafe.scalalogging.LazyLogging
import scala.language.implicitConversions
import com.campudus.tableaux.helper.JsonUtils.createAttributesValidator
import org.json.JSONObject;
import scala.util.{Failure, Success, Try}
import org.everit.json.schema.ValidationException;

class JsonSchemaValidatorVerticle extends ScalaVerticle with LazyLogging {
  private lazy val eventBus = vertx.eventBus()
  private lazy val validator = createAttributesValidator()
  override def startFuture(): Future[_] = {
    eventBus.consumer("json.schema.validate.attributes", messageHandlerValidateAttributes).completionFuture()
  }

  private def messageHandlerValidateAttributes(message: Message[String]): Unit = {
    val attributes = new JSONObject(message.body())
    val validated = Try(validator.validate(attributes)) match {
      case Success(v) => {
        message.reply("")
      }
      case Failure(e) => {
        message.fail(400, e.getMessage())
      }
    }
  }
}
