package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.testtools.AssertionHelpers
import org.junit.Assert._
import org.junit.Test
import org.vertx.scala.core.json.Json

abstract class AbstractTableDisplayInfosTest extends AssertionHelpers {

  @Test
  def checkSingleName(): Unit = {
    val di = singleName(1, "de_DE", "Tabelle 1")
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      "INSERT INTO system_table_lang (table_id, langtag, name, description) VALUES (?, ?, ?, ?)",
      statement
    )
    assertEquals(Seq(1, "de_DE", "Tabelle 1", null), binds)
  }

  @Test
  def checkMultipleNames(): Unit = {
    val di = multipleNames(1, List("de_DE" -> "Tabelle 1", "en_US" -> "Table 1"))
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      "INSERT INTO system_table_lang (table_id, langtag, name, description) VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
      statement
    )
    checkPartsInRandomOrder(Seq(
      Seq(1, "de_DE", "Tabelle 1", null),
      Seq(1, "en_US", "Table 1", null)
    ), binds)
  }

  @Test
  def checkSingleDescription(): Unit = {
    val di = singleDesc(1, "de_DE", "Tabelle 1 Beschreibung")
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      "INSERT INTO system_table_lang (table_id, langtag, name, description) VALUES (?, ?, ?, ?)",
      statement
    )
    assertEquals(Seq(1, "de_DE", null, "Tabelle 1 Beschreibung"), binds)
  }

  @Test
  def checkMultipleDescriptions(): Unit = {
    val di = multipleDescs(1, List("de_DE" -> "Tabelle 1 Beschreibung", "en_US" -> "Table 1 Description"))
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      "INSERT INTO system_table_lang (table_id, langtag, name, description) VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
      statement
    )
    checkPartsInRandomOrder(Seq(
      Seq(1, "de_DE", null, "Tabelle 1 Beschreibung"),
      Seq(1, "en_US", null, "Table 1 Description")
    ), binds)
  }

  @Test
  def checkSingleNameAndDescription(): Unit = {
    val di = singleNameAndDesc(1, "de_DE", "Tabelle 1", "Tabelle 1 Beschreibung")
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      "INSERT INTO system_table_lang (table_id, langtag, name, description) VALUES (?, ?, ?, ?)",
      statement
    )
    assertEquals(Seq(1, "de_DE", "Tabelle 1", "Tabelle 1 Beschreibung"), binds)
  }

  @Test
  def checkMultipleNamesAndDescriptions(): Unit = {
    val di = multipleNameAndDesc(1, List(
      ("de_DE", "Tabelle 1", "Tabelle 1 Beschreibung"),
      ("en_US", "Table 1", "Table 1 Description")
    ))
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      "INSERT INTO system_table_lang (table_id, langtag, name, description) VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
      statement
    )
    checkPartsInRandomOrder(Seq(
      Seq(1, "de_DE", "Tabelle 1", "Tabelle 1 Beschreibung"),
      Seq(1, "en_US", "Table 1", "Table 1 Description")
    ), binds)
  }

  @Test
  def checkNameAndOtherDesc(): Unit = {
    val di = multipleNameAndDesc(1, List(("de_DE", "Tabelle 1", null), ("en_US", null, "Table 1 Description")))
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      "INSERT INTO system_table_lang (table_id, langtag, name, description) VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
      statement
    )
    checkPartsInRandomOrder(Seq(
      Seq(1, "de_DE", "Tabelle 1", null),
      Seq(1, "en_US", null, "Table 1 Description")
    ), binds)
  }

  @Test
  def checkCombinations(): Unit = {
    val di = multipleNameAndDesc(1, List(
      ("de_DE", "Tabelle 1", "Tabelle 1 Beschreibung"),
      ("en_US", null, "Table 1 Description"),
      ("fr_FR", "Tableau 1", null)
    ))
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      "INSERT INTO system_table_lang (table_id, langtag, name, description) VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)",
      statement
    )
    val all = Seq(
      Seq(1, "de_DE", "Tabelle 1", "Tabelle 1 Beschreibung"),
      Seq(1, "en_US", null, "Table 1 Description"),
      Seq(1, "fr_FR", "Tableau 1", null)
    )

    checkPartsInRandomOrder(all, binds)
  }

  @Test
  def emptyDisplayStuff(): Unit = {
    val di = emptyDisplayInfo(1)
    assertFalse(di.nonEmpty)
  }

  def emptyDisplayInfo(tableId: TableId): TableDisplayInfos

  def singleName(tableId: TableId, langtag: String, name: String): TableDisplayInfos

  def multipleNames(tableId: TableId, langNames: List[(String, String)]): TableDisplayInfos

  def singleDesc(tableId: TableId, langtag: String, desc: String): TableDisplayInfos

  def multipleDescs(tableId: TableId, langDescs: List[(String, String)]): TableDisplayInfos

  def singleNameAndDesc(tableId: TableId, langtag: String, name: String, desc: String): TableDisplayInfos

  def multipleNameAndDesc(tableId: TableId, infos: List[(String, String, String)]): TableDisplayInfos

}

class TableDisplayInfosTestDirect extends AbstractTableDisplayInfosTest {

  override def emptyDisplayInfo(tableId: TableId): TableDisplayInfos =
    DisplayInfos(tableId, List())

  override def singleName(tableId: TableId, langtag: String, name: String): TableDisplayInfos =
    DisplayInfos(tableId, List(NameOnly(langtag, name)))

  override def multipleNames(tableId: TableId, langNames: List[(String, String)]): TableDisplayInfos =
    DisplayInfos(tableId, langNames.map(t => NameOnly(t._1, t._2)))

  override def singleDesc(tableId: TableId, langtag: String, desc: String): TableDisplayInfos =
    DisplayInfos(tableId, List(DescriptionOnly(langtag, desc)))

  override def multipleDescs(tableId: TableId, langDescs: List[(String, String)]): TableDisplayInfos =
    DisplayInfos(tableId, langDescs.map(t => DescriptionOnly(t._1, t._2)))

  override def singleNameAndDesc(tableId: TableId, langtag: String, name: String, desc: String): TableDisplayInfos =
    DisplayInfos(tableId, List(NameAndDescription(langtag, name, desc)))

  override def multipleNameAndDesc(tableId: TableId, infos: List[(String, String, String)]): TableDisplayInfos =
    DisplayInfos(tableId, infos.map {
      case (lang, name, null) => NameOnly(lang, name)
      case (lang, null, desc) => DescriptionOnly(lang, desc)
      case (lang, name, desc) => NameAndDescription(lang, name, desc)
    })

}

class TableDisplayInfosTestJsonObject extends AbstractTableDisplayInfosTest {

  override def emptyDisplayInfo(tableId: TableId): TableDisplayInfos =
    DisplayInfos(tableId, Json.obj())

  override def singleName(tableId: TableId, langtag: String, name: String): TableDisplayInfos =
    DisplayInfos(tableId, Json.obj("displayName" -> Json.obj(langtag -> name)))

  override def multipleNames(tableId: TableId, langNames: List[(String, String)]): TableDisplayInfos = {
    DisplayInfos(tableId, Json.obj("displayName" -> langNames.foldLeft(Json.obj()) {
      case (json, (langtag, name)) => json.mergeIn(Json.obj(langtag -> name))
    }))
  }

  override def singleDesc(tableId: TableId, langtag: String, desc: String): TableDisplayInfos =
    DisplayInfos(tableId, Json.obj("description" -> Json.obj(langtag -> desc)))

  override def multipleDescs(tableId: TableId, langDescs: List[(String, String)]): TableDisplayInfos =
    DisplayInfos(tableId, Json.obj("description" -> langDescs.foldLeft(Json.obj()) {
      case (json, (langtag, desc)) => json.mergeIn(Json.obj(langtag -> desc))
    }))

  override def singleNameAndDesc(tableId: TableId, langtag: String, name: String, desc: String): TableDisplayInfos =
    DisplayInfos(tableId, Json.obj(
      "displayName" -> Json.obj(langtag -> name),
      "description" -> Json.obj(langtag -> desc)
    ))

  override def multipleNameAndDesc(tableId: TableId, infos: List[(String, String, String)]): TableDisplayInfos = {
    val result = infos.foldLeft(Json.obj()) {
      case (json, (lang, name, null)) => json.mergeIn(Json.obj("displayName" -> json.getJsonObject("displayName", Json.obj()).mergeIn(Json.obj(lang -> name))))
      case (json, (lang, null, desc)) => json.mergeIn(Json.obj("description" -> json.getJsonObject("description", Json.obj()).mergeIn(Json.obj(lang -> desc))))
      case (json, (lang, name, desc)) =>
        val n = json.getJsonObject("displayName", Json.obj()).mergeIn(Json.obj(lang -> name))
        val d = json.getJsonObject("description", Json.obj()).mergeIn(Json.obj(lang -> desc))
        json.mergeIn(Json.obj("displayName" -> n)).mergeIn(Json.obj("description" -> d))
    }
    DisplayInfos(tableId, result)
  }

}