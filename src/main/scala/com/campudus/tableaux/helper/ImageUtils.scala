package com.campudus.tableaux.helper

import java.awt.RenderingHints
import java.awt.image.BufferedImage

object ImageUtils {

  def resizeImage(
      image: BufferedImage,
      targetWidth: Int,
      targetHeight: Int,
      imageType: Int = BufferedImage.TYPE_INT_ARGB
  ): BufferedImage = {
    val targetImage = new BufferedImage(targetWidth, targetHeight, imageType)
    val graphics = targetImage.createGraphics()

    graphics.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC)
    graphics.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY)
    graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

    graphics.drawImage(image, 0, 0, targetWidth, targetHeight, null)
    graphics.dispose()

    targetImage
  }

  def resizeImageSmooth(
      image: BufferedImage,
      targetWidth: Int,
      targetHeight: Int,
      imageType: Int = BufferedImage.TYPE_INT_ARGB
  ): BufferedImage = {

    def downscaleSteps(width: Int, height: Int, acc: List[(Int, Int)]): List[(Int, Int)] = {
      if (width > targetWidth * 1.5 || height > targetHeight * 1.5) {
        val nextWidth = Math.max(targetWidth, (width / 1.5).toInt)
        val nextHeight = Math.max(targetHeight, (height / 1.5).toInt)
        downscaleSteps(nextWidth, nextHeight, (nextWidth, nextHeight) :: acc)
      } else acc.reverse
    }

    val steps = downscaleSteps(image.getWidth, image.getHeight, Nil)

    val intermediate = steps.foldLeft(image) { case (img, (width, height)) =>
      resizeImage(img, width, height, imageType)
    }

    resizeImage(intermediate, targetWidth, targetHeight, imageType)
  }
}
