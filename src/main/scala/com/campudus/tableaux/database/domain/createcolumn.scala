package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.model.TableauxModel._

sealed trait CreateColumn {
  val name: String
  val kind: TableauxDbType
  val languageType: LanguageType
  val ordering: Option[Ordering]
}

case class CreateSimpleColumn(override val name: String,
                              override val ordering: Option[Ordering],
                              override val kind: TableauxDbType,
                              override val languageType: LanguageType) extends CreateColumn

case class CreateLinkColumn(override val name: String,
                            override val ordering: Option[Ordering],
                            linkConnection: LinkConnection,
                            toName: Option[String],
                            singleDirection: Boolean) extends CreateColumn {
  override val kind = LinkType
  override val languageType = SingleLanguage
}

case class CreateAttachmentColumn(override val name: String,
                                  override val ordering: Option[Ordering]) extends CreateColumn {
  override val kind = AttachmentType
  override val languageType = SingleLanguage
}