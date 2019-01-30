package org.vertx.scala.router.routing

import com.campudus.tableaux.router.RouterException
import org.vertx.scala.core.json._

import scala.concurrent.Future

sealed trait Reply

sealed trait SyncReply extends Reply

case object NoBody extends SyncReply

case class Ok(json: JsonObject) extends SyncReply

case class OkString(string: String, contentType: String = "application/json") extends SyncReply

case class SendFile(file: String, absolute: Boolean = false) extends SyncReply

case class SendEmbeddedFile(path: String) extends SyncReply

case class Error(ex: RouterException) extends SyncReply

case class AsyncReply(replyWhenDone: Future[Reply]) extends Reply

case class Header(key: String, value: String, endReply: Reply) extends Reply

case class SetCookie(key: String, value: String, endReply: Reply) extends Reply

case class StatusCode(statusCode: Int, endReply: Reply) extends Reply
