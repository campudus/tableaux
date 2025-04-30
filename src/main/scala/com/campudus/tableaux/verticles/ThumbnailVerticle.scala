package com.campudus.tableaux.verticles

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.FileModel
import com.campudus.tableaux.helper.{FileUtils, VertxAccess}
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.verticles.EventClient._

import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.SQLConnection
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.Message
import io.vertx.scala.ext.web.client.WebClient
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}

import com.twelvemonkeys.image.ResampleOp
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

  private val enableCacheWarmup = getBooleanDefault(thumbnailsConfig, "enableCacheWarmup", false);
  private val cacheWarmupWidths = asSeqOf[Int](thumbnailsConfig.getJsonArray("cacheWarmupWidths", Json.emptyArr()))
  private val cacheWarmupChunkSize = getIntDefault(thumbnailsConfig, "cacheWarmupChunkSize", 50);

  private val msIn6hours = 6 * 60 * 60 * 1000; // 21600000
  private val cacheClearPollingInterval = getIntDefault(thumbnailsConfig, "cacheClearPollingInterval", msIn6hours);

  private val secondsIn30Days = 30 * 24 * 60 * 60 // 2592000
  private val cacheMaxAge = getIntDefault(thumbnailsConfig, "cacheMaxAge", secondsIn30Days);
  private val cacheMaxAgePeriod = Period.seconds(cacheMaxAge).normalizedStandard(PeriodType.dayTime());
  private val cacheMaxAgeReadable = PeriodFormat.getDefault.print(cacheMaxAgePeriod) // e.g. "30 days"

  private val oldFileFilter = new FileFilter {

    override def accept(file: File): Boolean = {
      val now = Instant.now()
      val fileLastModified = Instant.ofEpochMilli(file.lastModified)
      val fileAge = Duration.between(fileLastModified, now).toSeconds.intValue

      fileAge > cacheMaxAge
    }
  }

  private def vertxAccess(): VertxAccess = new VertxAccess {
    override val vertx: Vertx = ThumbnailVerticle.this.vertx
  }

  override def startFuture(): Future[_] = {
    logger.info("start future")

    vertx.setPeriodic(cacheClearPollingInterval, _ => clearOldThumbnails())

    val vertxAccess = this.vertxAccess()

    val connection = SQLConnection(vertxAccess, tableauxConfig.databaseConfig)
    val dbConnection = DatabaseConnection(vertxAccess, connection)

    fileModel = FileModel(dbConnection)

    if (enableCacheWarmup) {
      for {
        _ <- this.generateThumbnailsForExistingImages()
      } yield {
        logger.info(s"Cache warmup for thumbnails complete")
      }
    }

    eventBus.consumer(ADDRESS_THUMBNAIL_RETRIEVE, retrieveThumbnailPath).completionFuture()
  }

  private def getIntDefault(config: JsonObject, field: String, default: Int): Int = {
    if (config.containsKey(field)) {
      config.getInteger(field).intValue
    } else {
      logger.warn(s"No $field (config) was set. Use default '$default'.")
      default
    }
  }

  private def getBooleanDefault(config: JsonObject, field: String, default: Boolean): Boolean = {
    if (config.containsKey(field)) {
      config.getBoolean(field).booleanValue
    } else {
      logger.warn(s"No $field (config) was set. Use default '$default'.")
      default
    }
  }

  private def createThumbnailsDirectory(): Future[Unit] = {
    FileUtils(this.vertxAccess).mkdirs(thumbnailsDirectoryPath)
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

  private def retrieveThumbnailPath(
      fileUuid: UUID,
      langtag: String,
      width: Int,
      enableLogs: Boolean = true
  ): Future[Option[Path]] = {
    for {
      _ <- createThumbnailsDirectory()
      file <- fileModel.retrieve(fileUuid)
      mimeType = file.mimeType.get(langtag).getOrElse("")
      internalName = file.internalName.get(langtag).getOrElse("")
      extension = Path(internalName).extension
      internalUuid = internalName.replace(s".$extension", "")
      filePath = uploadsDirectoryPath / Path(internalName)
      thumbnailName = s"${internalUuid}_$width.png" // thumbnail is always png
      thumbnailPath = thumbnailsDirectoryPath / Path(thumbnailName)
      doesFileExist <- checkExistence(filePath)
      doesThumbnailExist <- checkExistence(thumbnailPath)
      isMimeTypeValid = isValidMimeType(mimeType)
    } yield {
      (doesFileExist, doesThumbnailExist, isMimeTypeValid) match {
        case (true, true, true) => {
          if (enableLogs) logger.info(s"Updating timestamps for thumbnail $thumbnailName")

          Files.setLastModifiedTime(thumbnailPath.jfile.toPath, FileTime.from(Instant.now()))

          Some(thumbnailPath.toString)
        }
        case (true, false, true) => {
          if (enableLogs) logger.info(s"Creating thumbnail $thumbnailName")

          val baseFile = new File(filePath.toString)
          val baseImage = ImageIO.read(baseFile)
          val baseWidth = baseImage.getWidth;
          val baseHeight = baseImage.getHeight;
          val targetWidth = width;
          val targetHeight = (baseHeight.toFloat / baseWidth.toFloat) * targetWidth
          val resizeOp = new ResampleOp(targetWidth, targetHeight.toInt, ResampleOp.FILTER_LANCZOS);
          val targetImage = resizeOp.filter(baseImage, null)
          val targetFile = new File(thumbnailPath.toString)

          ImageIO.write(targetImage, "png", targetFile)

          Some(thumbnailPath.toString)
        }
        case (false, _, _) => None // file doesn't exist (only in dev)
        case (_, _, false) => None // unsupported mime type
      }
    }
  }

  private def retrieveThumbnailPath(message: Message[JsonObject]): Unit = {
    val uuid = message.body().getString("uuid")
    val fileUuid = UUID.fromString(uuid);
    val langtag = message.body().getString("langtag")
    val width = message.body().getInteger("width").intValue()

    for {
      thumbnailPath <- retrieveThumbnailPath(fileUuid, langtag, width)
    } yield {
      thumbnailPath match {
        case Some(path) => message.reply(path.toString)
        case None => message.fail(400, s"Unsupported mimeType")
      }
    }
  }

  private def clearOldThumbnails(): Unit = {
    val oldFiles = thumbnailsDirectoryPath.jfile.listFiles(oldFileFilter)

    logger.info(s"Clearing thumbnails older than $cacheMaxAgeReadable")

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

  private def generateThumbnailsForExistingImages(): Future[Seq[Option[Path]]] = {
    for {
      allFiles <- fileModel.retrieveAll()
      validFiles = allFiles.filter(file => file.mimeType.values.values.toSeq.exists(isValidMimeType))

      pathsFutures = for {
        file <- validFiles
        langtag <- file.internalName.langtags
        width <- cacheWarmupWidths
      } yield retrieveThumbnailPath(file.uuid, langtag, width, enableLogs = false)

      _ = logger.info(s"Generating/Updating ${pathsFutures.size} thumbnails for ${validFiles.size} existing files")
      _ = logger.info(s"Thumbnail widths: ${cacheWarmupWidths.mkString(", ")}")

      paths <- pathsFutures.grouped(cacheWarmupChunkSize).foldLeft(Future.successful(Seq.empty[Option[Path]])) {
        (pathsAcc, pathsChunk) =>
          for {
            acc <- pathsAcc
            chunk <- Future.sequence(pathsChunk)
            newAcc = acc ++ chunk
            _ = logger.info(s"Generated/Updated ${newAcc.count(_.isDefined)} of ${pathsFutures.size} thumbnails")
          } yield newAcc
      }
    } yield paths
  }
}
