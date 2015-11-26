package com.campudus.tableaux

sealed trait CustomException extends Throwable {
  val message: String

  val id: String

  val statusCode: Int

  override def toString: String = s"${super.toString}: $message"
}

case class NoJsonFoundException(override val message: String) extends CustomException {
  override val id = "error.json.notfound"
  override val statusCode = 400
}

case class NotFoundInDatabaseException(override val message: String, subId: String) extends CustomException {
  override val id = s"error.database.notfound.$subId"
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