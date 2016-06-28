package com.campudus.tableaux.database

sealed trait TableauxDbType {
  val name: String

  override def toString: String = name

  def toDbType: String = name
}

object TableauxDbType {
  def apply(kind: String): TableauxDbType = {
    kind match {
      case TextType.name => TextType
      case ShortTextType.name => ShortTextType
      case RichTextType.name => RichTextType
      case NumericType.name => NumericType
      case CurrencyType.name => CurrencyType
      case LinkType.name => LinkType
      case AttachmentType.name => AttachmentType
      case BooleanType.name => BooleanType
      case DateType.name => DateType
      case DateTimeType.name => DateTimeType
    }
  }
}

case object TextType extends TableauxDbType {
  override val name = "text"
}

case object RichTextType extends TableauxDbType {
  override val name = "richtext"

  override def toDbType = "text"
}

case object ShortTextType extends TableauxDbType {
  override val name = "shorttext"

  override def toDbType = "text"
}

case object NumericType extends TableauxDbType {
  override val name = "numeric"
}

case object LinkType extends TableauxDbType {
  override val name = "link"
}

case object AttachmentType extends TableauxDbType {
  override val name = "attachment"
}

case object BooleanType extends TableauxDbType {
  override val name = "boolean"
}

case object DateType extends TableauxDbType {
  override val name = "date"
}

case object DateTimeType extends TableauxDbType {
  override val name = "datetime"

  override def toDbType = "timestamp with time zone"
}

case object ConcatType extends TableauxDbType {
  override val name = "concat"
}

case object CurrencyType extends TableauxDbType {
  override val name = "currency"

  override def toDbType = "numeric"
}