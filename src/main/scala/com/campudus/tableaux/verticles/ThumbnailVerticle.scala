package com.campudus.tableaux.verticles

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.FileModel
import com.campudus.tableaux.helper.ImageUtils
import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.verticles.EventClient._

import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.SQLConnection
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.Message
import io.vertx.scala.ext.web.client.WebClient
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}

import com.typesafe.scalalogging.LazyLogging
import java.io.{File, FileFilter}
import java.nio.file.Files
import java.nio.file.attribute.FileTime
import java.time.{Duration, Instant}
import java.util.UUID
import javax.imageio.ImageIO
import org.joda.time.Period
import org.joda.time.PeriodType
import org.joda.time.format.PeriodFormat

class ThumbnailVerticle(thumbnailsConfig: JsonObject, tableauxConfig: TableauxConfig) extends ScalaVerticle
    with LazyLogging {
  private lazy val eventBus = vertx.eventBus()

  private var fileModel: FileModel = _

  private val uploadsDirectoryPath = tableauxConfig.uploadsDirectoryPath
  private val thumbnailsDirectoryPath = tableauxConfig.thumbnailsDirectoryPath

  private val secondsIn30Days = 30 * 24 * 60 * 60 // 2592000
  private val maxAgeSeconds = Option(thumbnailsConfig.getInteger("maxAge")).map(_.intValue).getOrElse(secondsIn30Days)
  private val maxAgePeriod = Period.seconds(maxAgeSeconds).normalizedStandard(PeriodType.dayTime());
  private val maxAgeReadable = PeriodFormat.getDefault.print(maxAgePeriod) // e.g. "30 days"

  private val oldFileFilter = new FileFilter {

    override def accept(file: File): Boolean = {
      val now = Instant.now()
      val fileLastModified = Instant.ofEpochMilli(file.lastModified)
      val fileAgeSeconds = Duration.between(fileLastModified, now).toSeconds.intValue

      fileAgeSeconds > maxAgeSeconds
    }
  }

  override def startFuture(): Future[_] = {
    logger.info("start future")

    vertx.setPeriodic(
      6 * 60 * 60 * 1000, // every 6 hours (in milliseconds)
      _ => clearOldThumbnails()
    )

    val vertxAccess = new VertxAccess {
      override val vertx: Vertx = ThumbnailVerticle.this.vertx
    }

    val connection = SQLConnection(vertxAccess, tableauxConfig.databaseConfig)
    val dbConnection = DatabaseConnection(vertxAccess, connection)

    fileModel = FileModel(dbConnection)

    eventBus.consumer(ADDRESS_THUMBNAIL_RETRIEVE, retrieveThumbnailPath).completionFuture()
  }

  private def checkExistence(thumbnailPath: Path): Future[Boolean] = {
    vertx
      .fileSystem()
      .existsFuture(thumbnailPath.toString)
  }

  private def isValidMimeType(mimeType: String): Boolean = {
    mimeType match {
      case "image/jpeg" | "image/png" | "image/webp" | "image/tiff" => true
      case _ => false
    }
  }

  private def retrieveThumbnailPath(message: Message[JsonObject]): Unit = {
    val uuid = message.body().getString("uuid")
    val fileUuid = UUID.fromString(uuid);
    val langtag = message.body().getString("langtag")
    val width = message.body().getInteger("width").intValue()

    for {
      file <- fileModel.retrieve(fileUuid)
      mimeType = file.mimeType.get(langtag).getOrElse("")
      internalName = file.internalName.get(langtag).getOrElse("")
      extension = Path(internalName).extension
      internalUuid = internalName.replace(s".$extension", "")
      filePath = uploadsDirectoryPath / Path(internalName)
      thumbnailName = s"${internalUuid}_$width.png" // thumbnail is always png
      thumbnailPath = thumbnailsDirectoryPath / Path(thumbnailName)
      doesThumbnailExist <- checkExistence(thumbnailPath)
      isMimeTypeValid = isValidMimeType(mimeType)
    } yield {
      (doesThumbnailExist, isMimeTypeValid) match {
        case (true, true) => {
          logger.info(s"Updating timestamps for thumbnail $thumbnailName")

          Files.setLastModifiedTime(thumbnailPath.jfile.toPath, FileTime.from(Instant.now()))

          message.reply(thumbnailPath.toString)
        }
        case (false, true) => {
          logger.info(s"Creating thumbnail $thumbnailName")

          val baseFile = new File(filePath.toString)
          val baseImage = ImageIO.read(baseFile)
          val baseWidth = baseImage.getWidth;
          val baseHeight = baseImage.getHeight;
          val targetWidth = width;
          val targetHeight = (baseHeight.toFloat / baseWidth.toFloat) * targetWidth
          val targetImage = ImageUtils.resizeImageSmooth(baseImage, targetWidth, targetHeight.toInt)
          val targetFile = new File(thumbnailPath.toString)

          ImageIO.write(targetImage, "png", targetFile)

          message.reply(thumbnailPath.toString)
        }
        case (_, false) => {
          message.fail(400, s"Unsupported mimeType '$mimeType'")
        }
      }
    }
  }

  private def clearOldThumbnails(): Unit = {
    val oldFiles = thumbnailsDirectoryPath.jfile.listFiles(oldFileFilter)

    logger.info(s"Clearing thumbnails older than $maxAgeReadable")

    for (oldFile <- oldFiles) {
      val deleteResult = Try(Files.delete(oldFile.toPath))

      deleteResult match {
        case Success(_) =>
          logger.info(s"Successfully deleted thumbnail ${oldFile.getName}")
        case Failure(ex) =>
          logger.info(s"Failed to delete thumbnail ${oldFile.getName}: ${ex.getMessage}")
      }
    }
  }
}
