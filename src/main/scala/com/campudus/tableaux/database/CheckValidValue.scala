package com.campudus.tableaux.database

object CheckValidValue {
  def boolToOption(b: Boolean): Option[String] = if (b) None else Some("value")
}
