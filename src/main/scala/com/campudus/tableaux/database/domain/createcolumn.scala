package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.model.TableauxModel._

sealed trait CreateColumn {
  val name: String
  val kind: TableauxDbType
  val languageType: LanguageType
  val ordering: Option[Ordering]
  val identifier: Boolean
  val frontendReadOnly: Boolean
  val displayInfos: Seq[DisplayInfo]
}

case class CreateSimpleColumn(override val name: String,
                              override val ordering: Option[Ordering],
                              override val kind: TableauxDbType,
                              override val languageType: LanguageType,
                              override val identifier: Boolean,
                              override val frontendReadOnly: Boolean,
                              override val displayInfos: Seq[DisplayInfo])
    extends CreateColumn

case class CreateBackLinkColumn(
    name: Option[String],
    ordering: Option[Ordering],
    displayInfos: Option[Seq[DisplayInfo]]
)

object CreateLinkColumn {

  def apply(name: String,
            ordering: Option[Ordering],
            toTable: TableId,
            toName: Option[String],
            toDisplayInfos: Option[Seq[DisplayInfo]],
            singleDirection: Boolean,
            identifier: Boolean,
            frontendReadOnly: Boolean,
            displayInfos: Seq[DisplayInfo],
            constraint: Constraint): CreateLinkColumn = {
    val createBackLinkColumn = CreateBackLinkColumn(
      name = toName,
      displayInfos = toDisplayInfos,
      ordering = ordering
    )

    CreateLinkColumn(
      name,
      ordering,
      toTable,
      singleDirection,
      identifier,
      frontendReadOnly,
      displayInfos,
      constraint,
      createBackLinkColumn
    )
  }
}

case class CreateLinkColumn(override val name: String,
                            override val ordering: Option[Ordering],
                            toTable: TableId,
                            singleDirection: Boolean,
                            override val identifier: Boolean,
                            override val frontendReadOnly: Boolean,
                            override val displayInfos: Seq[DisplayInfo],
                            constraint: Constraint,
                            foreignLinkColumn: CreateBackLinkColumn)
    extends CreateColumn {
  override val kind = LinkType
  override val languageType = LanguageNeutral
}

case class CreateAttachmentColumn(override val name: String,
                                  override val ordering: Option[Ordering],
                                  override val identifier: Boolean,
                                  override val frontendReadOnly: Boolean,
                                  override val displayInfos: Seq[DisplayInfo])
    extends CreateColumn {
  override val kind = AttachmentType
  override val languageType = LanguageNeutral
}

case class CreateGroupColumn(override val name: String,
                             override val ordering: Option[Ordering],
                             override val identifier: Boolean,
                             override val frontendReadOnly: Boolean,
                             override val displayInfos: Seq[DisplayInfo],
                             groups: Seq[ColumnId])
    extends CreateColumn {
  override val kind: TableauxDbType = GroupType
  override val languageType: LanguageType = LanguageNeutral
}

case class CreatedColumnInformation(
    tableId: TableId,
    columnId: ColumnId,
    ordering: Ordering,
    displayInfos: Seq[DisplayInfo] = List()
)
