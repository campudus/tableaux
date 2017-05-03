package com.campudus.tableaux.database.domain

import java.util.UUID

import org.joda.time.DateTime
import org.junit.Assert._
import org.junit.Test
import org.vertx.scala.core.json.Json

class TableauxFileTest {

  @Test
  def testFileDomainModels(): Unit = {
    val file = TableauxFile(
      UUID.randomUUID(),
      folders = Seq(1, 2, 3),
      title = MultiLanguageValue("de-DE" -> "changed 1"),
      description = MultiLanguageValue("de-DE" -> "changed 1"),
      internalName = MultiLanguageValue("de-DE" -> "changed 1"),
      externalName = MultiLanguageValue("de-DE" -> "changed 1"),
      mimeType = MultiLanguageValue("de-DE" -> "changed 1"),
      createdAt = Some(DateTime.now()),
      updatedAt = Some(DateTime.now())
    )

    val tempFile = TemporaryFile(file)
    val extendedFile = ExtendedFile(file)

    assertEquals(file.uuid, tempFile.uuid)

    assertEquals(file.folders, tempFile.folders)

    assertEquals(file.title, tempFile.title)
    assertEquals(file.description, tempFile.description)

    assertEquals(file.internalName, tempFile.internalName)
    assertEquals(file.externalName, tempFile.externalName)

    assertEquals(file.mimeType, tempFile.mimeType)

    assertEquals(file.createdAt, tempFile.createdAt)
    assertEquals(file.updatedAt, tempFile.updatedAt)

    assertEquals(file.uuid, extendedFile.uuid)

    assertEquals(file.folders, extendedFile.folders)

    assertEquals(file.title, extendedFile.title)
    assertEquals(file.description, extendedFile.description)

    assertEquals(file.internalName, extendedFile.internalName)
    assertEquals(file.externalName, extendedFile.externalName)

    assertEquals(file.mimeType, extendedFile.mimeType)

    assertEquals(file.createdAt, extendedFile.createdAt)
    assertEquals(file.updatedAt, extendedFile.updatedAt)
  }

  @Test
  def testGetUrlFromExtendedFileWithoutExternalName(): Unit = {
    val baseFile = TableauxFile(
      UUID.randomUUID(),
      folders = Seq(1, 2, 3),
      title = MultiLanguageValue("de-DE" -> "changed 1"),
      description = MultiLanguageValue("de-DE" -> "changed 1"),
      internalName = MultiLanguageValue("de-DE" -> "internal.pdf"),
      externalName = MultiLanguageValue("de-DE" -> "filename.pdf"),
      mimeType = MultiLanguageValue("de-DE" -> "changed 1"),
      createdAt = Some(DateTime.now()),
      updatedAt = Some(DateTime.now())
    )

    val file = ExtendedFile(baseFile)
    assertEquals(Json.obj("de-DE" -> s"/files/${file.uuid}/de-DE/filename.pdf"), file.getJson.getJsonObject("url"))

    val noExternalFile = ExtendedFile(baseFile.copy(externalName = MultiLanguageValue.empty()))
    assertEquals(Json.obj("de-DE" -> s"/files/${file.uuid}/de-DE/internal.pdf"),
      noExternalFile.getJson.getJsonObject("url"))

    val multipleExternalFile = ExtendedFile(
      baseFile.copy(externalName = MultiLanguageValue("de-DE" -> "dateiname.pdf", "en-GB" -> "filename.pdf")))
    assertEquals(
      Json.obj(
        "de-DE" -> s"/files/${file.uuid}/de-DE/dateiname.pdf",
        "en-GB" -> s"/files/${file.uuid}/en-GB/filename.pdf"
      ),
      multipleExternalFile.getJson.getJsonObject("url")
    )

    val multipleInternalFile = ExtendedFile(
      baseFile.copy(internalName = MultiLanguageValue("de-DE" -> "intern.pdf", "en-GB" -> "internal.pdf")))
    assertEquals(
      Json.obj(
        "de-DE" -> s"/files/${file.uuid}/de-DE/filename.pdf",
        "en-GB" -> s"/files/${file.uuid}/en-GB/internal.pdf"
      ),
      multipleInternalFile.getJson.getJsonObject("url")
    )
  }
}
