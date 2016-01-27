package com.campudus.tableaux.database

object CheckValidValue {
  def boolToArgumentError(b: Boolean): Option[String] = if (b) None else Some("value")
}
