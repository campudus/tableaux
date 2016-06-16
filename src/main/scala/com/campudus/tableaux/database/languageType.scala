package com.campudus.tableaux.database

sealed trait LanguageType

object LanguageType {
  final val NEUTRAL = "neutral"
  final val LANGUAGE = "language"
  final val COUNTRY = "country"

  def apply(typeOption: Option[String]): LanguageType = {
    typeOption match {
      case Some(LANGUAGE) => MultiLanguage()
      case Some(COUNTRY) => MultiCountry(CountryCodes(Seq.empty))
      case Some(NEUTRAL) | None => LanguageNeutral()
      case _ => throw new IllegalArgumentException("Invalid argument for LanguageType.apply")
    }
  }
}

case class LanguageNeutral() extends LanguageType {
  override def toString = LanguageType.NEUTRAL
}

case class MultiLanguage() extends LanguageType {
  override def toString = LanguageType.LANGUAGE
}

case class CountryCodes(codes: Seq[String])

object MultiCountry {
  def unapply(languageType: LanguageType): Option[CountryCodes] = {
    languageType match {
      case t: MultiCountry =>
        Some(t.countryCodes)
      case _ =>
        None
    }
  }
}

case class MultiCountry(countryCodes: CountryCodes) extends LanguageType {
  override def toString = LanguageType.COUNTRY
}