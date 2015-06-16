package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{AttachmentType, LinkType, TableauxDbType}

sealed trait CreateColumn {
  val kind: TableauxDbType
}

case class CreateSimpleColumn(name: String, kind: TableauxDbType, ordering: Option[Ordering]) extends CreateColumn

case class CreateLinkColumn(name: String, ordering: Option[Ordering], linkConnection: LinkConnection) extends CreateColumn {
  override val kind = LinkType
}

case class CreateAttachmentColumn(name: String, ordering: Option[Ordering]) extends CreateColumn {
  override val kind: TableauxDbType = AttachmentType
}