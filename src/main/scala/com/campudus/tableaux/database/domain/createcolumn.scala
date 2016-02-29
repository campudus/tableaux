package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.model.TableauxModel._

sealed trait CreateColumn {
  val name: String
  val kind: TableauxDbType
  val languageType: LanguageType
  val ordering: Option[Ordering]
  val identifier: Boolean
  val displayInfos: Seq[DisplayInfo]
}

case class CreateSimpleColumn(override val name: String,
                              override val ordering: Option[Ordering],
                              override val kind: TableauxDbType,
                              override val languageType: LanguageType,
                              override val identifier: Boolean,
                              override val displayInfos: Seq[DisplayInfo]) extends CreateColumn

case class CreateLinkColumn(override val name: String,
                            override val ordering: Option[Ordering],
                            toTable: TableId,
                            toName: Option[String],
                            singleDirection: Boolean,
                            override val identifier: Boolean,
                            override val displayInfos: Seq[DisplayInfo]) extends CreateColumn {
  override val kind = LinkType
  override val languageType = SingleLanguage
}

case class CreateAttachmentColumn(override val name: String,
                                  override val ordering: Option[Ordering],
                                  override val identifier: Boolean,
                                  override val displayInfos: Seq[DisplayInfo]) extends CreateColumn {
  override val kind = AttachmentType
  override val languageType = SingleLanguage
}

case class ColumnInfo(tableId: TableId, columnId: ColumnId, ordering: Ordering, displayInfos: Seq[DisplayInfo] = List())
