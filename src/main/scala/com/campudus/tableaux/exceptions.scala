package com.campudus.tableaux

import org.vertx.scala.router.RouterException

sealed trait CustomException extends Throwable {
  val message: String

  val id: String

  val statusCode: Int

  override def toString: String = s"${super.toString}: $message"

  def toRouterException = RouterException(
    message = message,
    cause = this,
    id = id,
    statusCode = statusCode
  )
}

case class NoJsonFoundException(override val message: String) extends CustomException {
  override val id = "error.json.notfound"
  override val statusCode = 400
}

case class NotFoundInDatabaseException(msg: String, subId: String) extends CustomException {
  override val id = s"error.database.notfound.$subId"
  override val message = s"$id: $msg"
  override val statusCode = 404
}

case class DatabaseException(override val message: String, subId: String) extends CustomException {
  override val id = s"error.database.$subId"
  override val statusCode = 500
}

case class NotEnoughArgumentsException(override val message: String) extends CustomException {
  override val id = s"error.json.arguments"
  override val statusCode = 400
}

case class InvalidJsonException(override val message: String, subId: String) extends CustomException {
  override val id = s"error.json.$subId"
  override val statusCode = 400
}

case class InvalidNonceException(override val message: String) extends CustomException {
  override val id = "error.nonce.invalid"
  override val statusCode = 401
}

case class NoNonceException(override val message: String) extends CustomException {
  override val id = "error.nonce.none"
  override val statusCode = 500
}

case class ParamNotFoundException(override val message: String) extends CustomException {
  override val id = "error.param.notfound"
  override val statusCode = 400
}

case class InvalidRequestException(override val message: String) extends CustomException {
  override val id = "error.request.invalid"
  override val statusCode = 400
}

case class UnknownServerException(override val message: String) extends CustomException {
  override val id = "error.unknown"
  override val statusCode = 500
}