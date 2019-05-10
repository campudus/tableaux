package com.campudus.tableaux

import com.campudus.tableaux.database.domain.ColumnType
import com.campudus.tableaux.database.model.TableauxModel.{RowId, TableId}
import com.campudus.tableaux.router.RouterException

sealed trait CustomException extends Throwable {
  val message: String

  val id: String

  val statusCode: Int

  val cause: Throwable = None.orNull

  initCause(cause)

  override def getMessage: String = message

  def toRouterException: RouterException = RouterException(
    message = toString,
    cause = getCause,
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

case class RowNotFoundException(tableId: TableId, rowId: RowId) extends CustomException {
  override val id = s"error.database.notfound.row"
  override val message = s"Row $rowId not found in table $tableId"
  override val statusCode = 404
}

/**
  * The request was well-formed but was unable to be followed due to semantic errors.
  */
case class UnprocessableEntityException(override val message: String, override val cause: Throwable = None.orNull)
    extends CustomException {
  override val id = s"unprocessable.entity"
  override val statusCode = 422
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

case class UnknownServerException(override val message: String, override val cause: Throwable = None.orNull)
    extends CustomException {
  override val id = "error.unknown"
  override val statusCode = 500
}

case class ShouldBeUniqueException(override val message: String, subId: String) extends CustomException {
  override val id = s"error.request.unique.$subId"
  override val statusCode = 400
}

case class WrongColumnKindException[T <: ColumnType[_]](column: ColumnType[_], shouldBe: Class[T])
    extends CustomException {
  override val id: String = s"error.request.column.wrongtype"
  override val statusCode: Int = 400
  override val message: String =
    s"This action is not possible on ${column.name}. Action only available for columns of kind ${shouldBe.toString}."
}

case class ForbiddenException(override val message: String, subId: String) extends CustomException {
  override val id: String = s"error.request.forbidden.$subId"
  override val statusCode: Int = 403
}

case class AuthenticationException(override val message: String) extends CustomException {
  override val id: String = s"error.request.unauthorized"
  override val statusCode: Int = 401
}
