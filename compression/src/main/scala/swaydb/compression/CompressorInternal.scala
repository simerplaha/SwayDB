/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.compression

import java.nio.ByteBuffer

import com.typesafe.scalalogging.LazyLogging
import net.jpountz.lz4.{LZ4Compressor, LZ4Factory}
import org.xerial.snappy
import swaydb.ErrorHandler.Throwable
import swaydb.IO
import swaydb.data.compression.LZ4Compressor.{Fast, High}
import swaydb.data.compression.LZ4Instance
import swaydb.data.compression.LZ4Instance._
import swaydb.data.io.Core
import swaydb.data.slice.Slice

private[swaydb] sealed trait CompressorInternal {
  val minCompressionPercentage: Double

  def compress(slice: Slice[Byte]): IO[Core.IO.Error, Option[Slice[Byte]]]

  def compress(emptyHeadSpace: Int, slice: Slice[Byte]): IO[Core.IO.Error, Option[Slice[Byte]]]
}

private[swaydb] object CompressorInternal extends LazyLogging {

  def apply(instance: swaydb.data.compression.LZ4Instance,
            compressor: swaydb.data.compression.LZ4Compressor): CompressorInternal.LZ4 =
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

  private def lz4Compressor(compressor: swaydb.data.compression.LZ4Compressor,
                            factory: LZ4Factory): CompressorInternal.LZ4 =
    compressor match {
      case Fast(minCompressionPercentage) =>
        CompressorInternal.LZ4(minCompressionPercentage, factory.fastCompressor())

      case High(minCompressionPercentage, compressionLevel) =>
        compressionLevel match {
          case Some(compressionLevel) =>
            CompressorInternal.LZ4(minCompressionPercentage, factory.highCompressor(compressionLevel))
          case None =>
            CompressorInternal.LZ4(minCompressionPercentage, factory.highCompressor())
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
                                 compressor: LZ4Compressor) extends CompressorInternal {

    final val compressionName = this.getClass.getSimpleName

    override def compress(slice: Slice[Byte]): IO[Core.IO.Error, Option[Slice[Byte]]] =
      compress(
        emptyHeadSpace = 0,
        slice = slice
      )

    def compress(emptyHeadSpace: Int, slice: Slice[Byte]): IO[Core.IO.Error, Option[Slice[Byte]]] =
      IO {
        val maxCompressLength = compressor.maxCompressedLength(slice.size)
        val compressedBuffer = ByteBuffer.allocate(maxCompressLength + emptyHeadSpace)
        val compressedBytes = compressor.compress(slice.toByteBufferWrap, slice.fromOffset, slice.size, compressedBuffer, emptyHeadSpace, maxCompressLength)

        if (isCompressionSatisfied(minCompressionPercentage, compressedBytes, slice.size, compressionName))
          Some(
            Slice.from(
              byteBuffer = compressedBuffer,
              from = 0,
              to = emptyHeadSpace + compressedBytes - 1
            )
          )
        else
          None
      }
  }

  private[swaydb] case object UnCompressedGroup extends CompressorInternal {

    final val compressionName = this.getClass.getSimpleName.dropRight(1)

    override final val minCompressionPercentage: Double = Double.MinValue

    override def compress(emptyHeadSpace: Int, slice: Slice[Byte]): IO[Core.IO.Error, Option[Slice[Byte]]] =
      IO(Some(Slice.fill[Byte](emptyHeadSpace)(0) ++ slice))

    override def compress(slice: Slice[Byte]): IO[Core.IO.Error, Option[Slice[Byte]]] = {
      logger.debug(s"Grouped {}.bytes with {}", slice.size, compressionName)
      IO.Success(Some(slice))
    }
  }

  private[swaydb] case class Snappy(minCompressionPercentage: Double) extends CompressorInternal {

    final val compressionName = this.getClass.getSimpleName

    override def compress(slice: Slice[Byte]): IO[Core.IO.Error, Option[Slice[Byte]]] =
      compress(emptyHeadSpace = 0, slice = slice)

    override def compress(emptyHeadSpace: Int, slice: Slice[Byte]): IO[Core.IO.Error, Option[Slice[Byte]]] =
      IO {
        val compressedArray = new Array[Byte](snappy.Snappy.maxCompressedLength(slice.size) + emptyHeadSpace)
        val (bytes, fromOffset, written) = slice.underlyingWrittenArrayUnsafe
        val compressedSize = snappy.Snappy.compress(bytes, fromOffset, written, compressedArray, emptyHeadSpace)
        if (isCompressionSatisfied(minCompressionPercentage, compressedSize, slice.size, this.getClass.getSimpleName))
          Some(Slice(compressedArray).slice(0, emptyHeadSpace + compressedSize - 1))
        else
          None
      }
  }

  def randomLZ4(minCompressionSavingsPercent: Double = Double.MinValue): CompressorInternal.LZ4 =
    CompressorInternal(
      instance = LZ4Instance.random(),
      compressor = swaydb.data.compression.LZ4Compressor.random(minCompressionSavingsPercent = minCompressionSavingsPercent)
    )
}