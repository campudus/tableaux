package com.campudus.tableaux

sealed trait CustomException extends Throwable {
  def message: String

  def id: String

  override def toString: String = s"${super.toString()}: $message"
}

case class NoJsonFoundException(message: String, id: String) extends CustomException

case class NotFoundInDatabaseException(message: String, id: String) extends CustomException

case class DatabaseException(message: String, id: String) extends CustomException