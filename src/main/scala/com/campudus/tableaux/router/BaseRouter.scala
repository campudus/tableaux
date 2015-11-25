package com.campudus.tableaux.router

import com.campudus.tableaux._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.{EmptyReturn, GetReturn, ReturnType, SetReturn}
import com.campudus.tableaux.helper.VertxAccess
import com.typesafe.scalalogging.LazyLogging
import io.vertx.core.buffer.Buffer
import io.vertx.ext.web.RoutingContext
import io.vertx.scala.FunctionConverters._
import io.vertx.scala.FutureHelper._
import io.vertx.scala.ScalaVerticle
import org.vertx.scala.core.json._
import org.vertx.scala.router.routing.{AsyncReply, Error, Ok}
import org.vertx.scala.router.{Router, RouterException}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

trait BaseRouter extends Router with VertxAccess with LazyLogging {

  val config: TableauxConfig

  /**
    * Regex for a UUID Version 4
    */
  val uuidRegex: String = "[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"

  /**
    * Regex for a Language tag e.g. de_DE, de-DE, or en_GB
    */
  val langtagRegex: String = "[a-z]{2}[-_][A-Z]{2}"

  override val verticle: ScalaVerticle = config.verticle

  def asyncSetReply: (Future[DomainObject]) => AsyncReply = asyncReply(SetReturn)(_)

  def asyncGetReply: (Future[DomainObject]) => AsyncReply = asyncReply(GetReturn)(_)

  def asyncEmptyReply: (Future[DomainObject]) => AsyncReply = asyncReply(EmptyReturn)(_)

  def asyncReply(reType: ReturnType)(f: => Future[DomainObject]): AsyncReply = AsyncReply {
    f map { d => Ok(Json.obj("status" -> "ok").mergeIn(d.toJson(reType))) } recover {
      case ex: CustomException => Error(RouterException(ex.message, ex, ex.id, ex.statusCode))
      case ex: Throwable => Error(RouterException("unknown error", ex, "error.unknown", 500))
    }
  }

  def getJson(context: RoutingContext): Future[JsonObject] = futurify { p: Promise[JsonObject] =>
    context.request().bodyHandler({ buffer: Buffer =>
      val requestBody = buffer.toString()
      Option(requestBody).getOrElse("").isEmpty match {
        case true => p.failure(NoJsonFoundException("Warning: No Json found"))
        case false => Try(Json.fromObjectString(requestBody)) match {
          case Success(r) => p.success(r)
          case Failure(x) => p.failure(x)
        }
      }
    })
  }

  def getLongParam(name: String, context: RoutingContext): Option[Long] = {
    Option(context.request().getParam(name)).map(_.toLong)
  }

  def getStringParam(name: String, context: RoutingContext): Option[String] = {
    Option(context.request().getParam(name))
  }
}
