package com.campudus.tableaux.router

import com.campudus.tableaux._
import com.campudus.tableaux.database.structure.{GetReturn, EmptyReturn, SetReturn, ReturnType}
import com.campudus.tableaux.database.DomainObject
import com.campudus.tableaux.helper.StandardVerticle
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.core.json._
import org.vertx.scala.router.routing.{AsyncReply, Error, Ok}
import org.vertx.scala.router.{Router, RouterException}

import scala.concurrent.{Future, Promise}

trait BaseRouter extends Router with StandardVerticle {

  val asyncSetReply = asyncReply(SetReturn) _
  val asyncGetReply = asyncReply(GetReturn) _
  val asyncEmptyReply = asyncReply(EmptyReturn) _
  
  def asyncReply(reType: ReturnType)(f: => Future[DomainObject]): AsyncReply = AsyncReply {
    f map { d => Ok(Json.obj("status" -> "ok").mergeIn(d.toJson(reType))) } recover {
      case ex@NotFoundInDatabaseException(message, id) => Error(RouterException(message, ex, s"errors.database.$id", 404))
      case ex@DatabaseException(message, id) => Error(RouterException(message, ex, s"errors.database.$id", 500))
      case ex@NoJsonFoundException(message, id) => Error(RouterException(message, ex, s"errors.json.$id", 400))
      case ex@NotEnoughArgumentsException(message, id) => Error(RouterException(message, ex, s"error.json.$id", 400))
      case ex@InvalidJsonException(message, id) => Error(RouterException(message, ex, s"error.json.$id", 400))
      case ex: Throwable => Error(RouterException("unknown error", ex, "errors.unknown", 500))
    }
  }

  def getJson(req: HttpServerRequest): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    req.bodyHandler { buf =>
      buf.length() match {
        case 0 => p.failure(NoJsonFoundException("Warning: No Json found", "not-found"))
        case _ => p.success(Json.fromObjectString(buf.toString()))
      }
    }
    p.future
  }
}
