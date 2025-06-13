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
      case IntegerType.name => IntegerType
      case CurrencyType.name => CurrencyType
      case LinkType.name => LinkType
      case AttachmentType.name => AttachmentType
      case BooleanType.name => BooleanType
      case DateType.name => DateType
      case DateTimeType.name => DateTimeType
      case GroupType.name => GroupType
      case StatusType.name => StatusType
      case OriginTableType.name => OriginTableType
    }
  }
}

case object TextType extends TableauxDbType {
  override val name = "text"
}

case object RichTextType extends TableauxDbType {
  override val name = "richtext"

  override def toDbType: String = "text"
}

case object ShortTextType extends TableauxDbType {
  override val name = "shorttext"

  override def toDbType: String = "text"
}

case object NumericType extends TableauxDbType {
  override val name = "numeric"
}

case object IntegerType extends TableauxDbType {
  override val name = "integer"
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

  override def toDbType: String = "timestamp with time zone"
}

case object ConcatType extends TableauxDbType {
  override val name = "concat"
}

case object CurrencyType extends TableauxDbType {
  override val name = "currency"

  override def toDbType: String = "numeric"
}

case object GroupType extends TableauxDbType {
  override val name = "group"
}

case object StatusType extends TableauxDbType {
  override val name = "status"
}

case object OriginTableType extends TableauxDbType {
  override val name = "origintable"
}
