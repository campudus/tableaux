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
    var currentImage = image
    var currentWidth = image.getWidth
    var currentHeight = image.getHeight

    // resize image in multiple steps for better quality
    while (currentWidth > targetWidth * 1.5 || currentHeight > targetHeight * 1.5) {
      val nextWidth = Math.max(targetWidth, currentWidth / 1.5)
      val nextHeight = Math.max(targetHeight, currentHeight / 1.5)

      currentWidth = nextWidth.toInt
      currentHeight = nextHeight.toInt
      currentImage = resizeImage(currentImage, currentWidth, currentHeight, imageType)
    }

    val finalImage = resizeImage(currentImage, targetWidth, targetHeight, imageType)

    finalImage
  }
}
