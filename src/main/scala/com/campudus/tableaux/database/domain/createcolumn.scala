package com.campudus.tableaux.database.domain
import org.vertx.scala.core.json._

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.model.TableauxModel._

sealed trait CreateColumn {
  val name: String
  val kind: TableauxDbType
  val languageType: LanguageType
  val ordering: Option[Ordering]
  val identifier: Boolean
  val displayInfos: Seq[DisplayInfo]
  val separator: Boolean
  val attributes: Option[JsonObject]
}

case class CreateSimpleColumn(override val name: String,
                              override val ordering: Option[Ordering],
                              override val kind: TableauxDbType,
                              override val languageType: LanguageType,
                              override val identifier: Boolean,
                              override val displayInfos: Seq[DisplayInfo],
                              override val separator: Boolean,
                              override val attributes: Option[JsonObject])
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
            displayInfos: Seq[DisplayInfo],
            constraint: Constraint,
            attributes: Option[JsonObject]): CreateLinkColumn = {
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
      displayInfos,
      constraint,
      createBackLinkColumn,
      attributes
    )
  }
}

case class CreateLinkColumn(override val name: String,
                            override val ordering: Option[Ordering],
                            toTable: TableId,
                            singleDirection: Boolean,
                            override val identifier: Boolean,
                            override val displayInfos: Seq[DisplayInfo],
                            constraint: Constraint,
                            foreignLinkColumn: CreateBackLinkColumn,
                            override val attributes: Option[JsonObject])
    extends CreateColumn {
  override val kind: LinkType.type = LinkType
  override val languageType: LanguageNeutral.type = LanguageNeutral
  override val separator: Boolean = false
}

case class CreateAttachmentColumn(override val name: String,
                                  override val ordering: Option[Ordering],
                                  override val identifier: Boolean,
                                  override val displayInfos: Seq[DisplayInfo],
                                  override val attributes: Option[JsonObject])
    extends CreateColumn {
  override val kind: AttachmentType.type = AttachmentType
  override val languageType: LanguageNeutral.type = LanguageNeutral
  override val separator: Boolean = false
}

case class CreateGroupColumn(override val name: String,
                             override val ordering: Option[Ordering],
                             override val identifier: Boolean,
                             formatPattern: Option[String],
                             override val displayInfos: Seq[DisplayInfo],
                             groups: Seq[ColumnId],
                             override val attributes: Option[JsonObject])
    extends CreateColumn {
  override val kind: TableauxDbType = GroupType
  override val languageType: LanguageType = LanguageNeutral
  override val separator: Boolean = false
}

case class CreatedColumnInformation(
    tableId: TableId,
    columnId: ColumnId,
    ordering: Ordering,
    displayInfos: Seq[DisplayInfo] = List()
)
