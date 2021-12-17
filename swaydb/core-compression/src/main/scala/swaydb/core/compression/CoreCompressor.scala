/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.compression

import com.typesafe.scalalogging.LazyLogging
import net.jpountz.lz4.{LZ4Compressor, LZ4Factory}
import org.xerial.snappy
import swaydb.config.compression.LZ4Compressor.{Fast, High}
import swaydb.config.compression.LZ4Instance
import swaydb.config.compression.LZ4Instance._
import swaydb.slice.Slice

import java.nio.ByteBuffer

private[swaydb] sealed trait CoreCompressor {
  val minCompressionPercentage: Double

  def compress(slice: Slice[Byte]): Option[Slice[Byte]]

  def compress(emptyHeadSpace: Int, slice: Slice[Byte]): Option[Slice[Byte]]
}

private[swaydb] object CoreCompressor extends LazyLogging {

  def apply(instance: swaydb.config.compression.LZ4Instance,
            compressor: swaydb.config.compression.LZ4Compressor): CoreCompressor.LZ4 =
    lz4Compressor(
      compressor = compressor,
      factory = lz4Factory(instance)
    )

  private def lz4Factory(instance: LZ4Instance): LZ4Factory =
    instance match {
      //@formatter:off
      case Fastest =>     LZ4Factory.fastestInstance()
      case FastestJava => LZ4Factory.fastestJavaInstance()
      case Native =>      LZ4Factory.nativeInstance()
      case Safe =>        LZ4Factory.safeInstance()
      case Unsafe =>      LZ4Factory.unsafeInstance()
      //@formatter:on
    }

  private def lz4Compressor(compressor: swaydb.config.compression.LZ4Compressor,
                            factory: LZ4Factory): CoreCompressor.LZ4 =
    compressor match {
      case Fast(minCompressionPercentage) =>
        CoreCompressor.LZ4(minCompressionPercentage, factory.fastCompressor())

      case High(minCompressionPercentage, compressionLevel) =>
        compressionLevel match {
          case Some(compressionLevel) =>
            CoreCompressor.LZ4(minCompressionPercentage, factory.highCompressor(compressionLevel))

          case None =>
            CoreCompressor.LZ4(minCompressionPercentage, factory.highCompressor())
        }
    }

  /**
   * @return true if the compression satisfies the minimum compression requirement else false.
   */
  def isCompressionSatisfied(minCompressionPercentage: Double,
                             compressedLength: Int,
                             originalLength: Int,
                             compressionName: String): Boolean = {
    val compressionSavedPercentage = (1D - (compressedLength.toDouble / originalLength.toDouble)) * 100
    if (compressionSavedPercentage < minCompressionPercentage) {
      logger.debug(s"Uncompressed! $compressionName - $originalLength.bytes compressed to $compressedLength.bytes. Compression savings = $compressionSavedPercentage%. Required minimum $minCompressionPercentage%.")
      false
    } else {
      logger.debug(s"Compressed! $compressionName - $originalLength.bytes compressed to $compressedLength.bytes. Compression savings = $compressionSavedPercentage%. Required minimum $minCompressionPercentage%.")
      true
    }
  }

  private[swaydb] case class LZ4(minCompressionPercentage: Double,
                                 compressor: LZ4Compressor) extends CoreCompressor {

    final val compressionName = this.productPrefix

    override def compress(slice: Slice[Byte]): Option[Slice[Byte]] =
      compress(
        emptyHeadSpace = 0,
        slice = slice
      )

    def compress(emptyHeadSpace: Int, slice: Slice[Byte]): Option[Slice[Byte]] = {
      val maxCompressLength = compressor.maxCompressedLength(slice.size)
      val compressedBuffer = ByteBuffer.allocate(maxCompressLength + emptyHeadSpace)
      val compressedBytes = compressor.compress(slice.toByteBufferWrap(), slice.fromOffset, slice.size, compressedBuffer, emptyHeadSpace, maxCompressLength)

      if (isCompressionSatisfied(minCompressionPercentage, compressedBytes, slice.size, compressionName))
        Some(
          Slice.wrap(
            byteBuffer = compressedBuffer,
            from = 0,
            to = emptyHeadSpace + compressedBytes - 1
          )
        )
      else
        None
    }
  }

  private[swaydb] case object UnCompressed extends CoreCompressor {

    final val compressionName = this.productPrefix

    override final val minCompressionPercentage: Double = Double.MinValue

    override def compress(emptyHeadSpace: Int, slice: Slice[Byte]): Option[Slice[Byte]] =
      Some(Slice.fill[Byte](emptyHeadSpace)(0) ++ slice)

    override def compress(slice: Slice[Byte]): Option[Slice[Byte]] = {
      logger.debug(s"Uncompressed {}.bytes with {}", slice.size, compressionName)
      Some(slice)
    }
  }

  private[swaydb] case class Snappy(minCompressionPercentage: Double) extends CoreCompressor {

    final val compressionName = this.productPrefix

    override def compress(slice: Slice[Byte]): Option[Slice[Byte]] =
      compress(emptyHeadSpace = 0, slice = slice)

    override def compress(emptyHeadSpace: Int, slice: Slice[Byte]): Option[Slice[Byte]] = {
      val compressedArray = new Array[Byte](snappy.Snappy.maxCompressedLength(slice.size) + emptyHeadSpace)
      val (bytes, fromOffset, written) = slice.underlyingWrittenArrayUnsafe
      val compressedSize = snappy.Snappy.compress(bytes, fromOffset, written, compressedArray, emptyHeadSpace)
      if (isCompressionSatisfied(minCompressionPercentage, compressedSize, slice.size, this.productPrefix))
        Some(Slice.wrap(compressedArray).slice(0, emptyHeadSpace + compressedSize - 1))
      else
        None
    }
  }
}
