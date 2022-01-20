package com.campudus.tableaux

import com.campudus.tableaux.database.domain.ColumnType
import com.campudus.tableaux.database.model.TableauxModel.{RowId, TableId}
import com.campudus.tableaux.router.RouterException
import com.campudus.tableaux.router.auth.permission.{Action, Scope}
import com.campudus.tableaux.database.{TableauxDbType, LanguageType}

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

case class ColumnNotFoundException(override val message: String) extends CustomException {
  override val id = s"error.json.column"
  override val statusCode = 404
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

case class WrongJsonTypeException(override val message: String) extends CustomException {
  override val id = s"error.request.json.wrongtype"
  override val statusCode = 400
}

case class WrongColumnKindException[T <: ColumnType[_]](column: ColumnType[_], shouldBe: Class[T])
    extends CustomException {
  override val id: String = s"error.request.column.wrongtype"
  override val statusCode: Int = 400
  override val message: String =
    s"This action is not possible on ${column.name}. Action only available for columns of kind ${shouldBe.toString}."
}

case class WrongStatusColumnKindException(wrongColumn: ColumnType[_], shouldBe: Seq[TableauxDbType])
    extends CustomException {
  override val id: String = s"error.request.column.wrongtype"
  override val statusCode: Int = 400
  override val message: String =
    s"This action is not possible on Column with kind: ${wrongColumn.kind}. Action only available for columns of kind ${shouldBe.toString}."
}

case class WrongLanguageTypeException(wrongColumn: ColumnType[_], shouldBe: LanguageType) extends CustomException {
  override val id: String = s"error.request.column.wrongtype"
  override val statusCode: Int = 400
  override val message: String =
    s"This action is not possible on Columns with LanguageType:  ${wrongColumn.languageType}. Action only available for columns of LanguageType ${shouldBe.toString}."
}

case class WrongStatusConditionTypeException(column: ColumnType[_], is: String, shouldBe: String)
    extends CustomException {
  override val id: String = s"error.request.status.value.wrongtype"
  override val statusCode: Int = 400
  override val message: String =
    s"Type of condition value does not match column type. Value for column ${column.id} is ${is} but should be ${shouldBe}."
}

case class ForbiddenException(override val message: String, subId: String) extends CustomException {
  override val id: String = s"error.request.forbidden.$subId"
  override val statusCode: Int = 403
}

case class AuthenticationException(override val message: String) extends CustomException {
  override val id: String = s"error.request.unauthenticated"
  override val statusCode: Int = 401
}

case class UnauthorizedException(action: Action, scope: Scope) extends CustomException {
  override val id: String = s"error.request.unauthorized"
  override val statusCode: Int = 403
  override val message: String = s"Action $action on scope $scope is not allowed."
}

case class HasStatusColumnDependencyException(override val message: String) extends CustomException {
  override val id: String = s"error.column.dependency"
  override val statusCode: Int = 409
}
