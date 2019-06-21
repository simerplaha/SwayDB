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
import swaydb.data.IO
import swaydb.data.compression.LZ4Compressor.{FastCompressor, HighCompressor}
import swaydb.data.compression.LZ4Instance
import swaydb.data.compression.LZ4Instance._
import swaydb.data.slice.Slice
import swaydb.data.util.PipeOps._

private[swaydb] sealed trait CompressorInternal {
  val minCompressionPercentage: Double

  def compress(slice: Slice[Byte]): IO[Option[Slice[Byte]]]
}

private[swaydb] object CompressorInternal extends LazyLogging {

  def apply(instance: swaydb.data.compression.LZ4Instance,
            compressor: swaydb.data.compression.LZ4Compressor): CompressorInternal.LZ4 =
    lz4Factory(instance) ==> {
      factory =>
        lz4Compressor(compressor, factory)
    }

  private def lz4Factory(instance: LZ4Instance): LZ4Factory =
    instance match {
      //@formatter:off
      case FastestInstance =>     LZ4Factory.fastestInstance()
      case FastestJavaInstance => LZ4Factory.fastestJavaInstance()
      case NativeInstance =>      LZ4Factory.nativeInstance()
      case SafeInstance =>        LZ4Factory.safeInstance()
      case UnsafeInstance =>      LZ4Factory.unsafeInstance()
      //@formatter:on
    }

  private def lz4Compressor(compressor: swaydb.data.compression.LZ4Compressor,
                            factory: LZ4Factory): CompressorInternal.LZ4 =
    compressor match {
      case FastCompressor(minCompressionPercentage) =>
        CompressorInternal.LZ4(minCompressionPercentage, factory.fastCompressor())

      case HighCompressor(minCompressionPercentage, compressionLevel) =>
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
      logger.debug(s"Uncompressed! $compressionName - $originalLength.bytes compressed to $compressedLength.bytes. Compression savings = $compressionSavedPercentage%. Required minimum $minCompressionPercentage%")
      false
    } else {
      logger.debug(s"Compressed! $compressionName - $originalLength.bytes compressed to $compressedLength.bytes. Compression savings = $compressionSavedPercentage%. Required minimum $minCompressionPercentage%")
      true
    }
  }

  private[swaydb] case class LZ4(minCompressionPercentage: Double,
                                 compressor: LZ4Compressor) extends CompressorInternal {

    val compressionName = this.getClass.getSimpleName

    override def compress(slice: Slice[Byte]): IO[Option[Slice[Byte]]] =
      IO {
        val compressedBuffer = ByteBuffer.allocate(compressor.maxCompressedLength(slice.written))
        compressor.compress(slice.toByteBufferWrap, compressedBuffer)

        if (isCompressionSatisfied(minCompressionPercentage, compressedBuffer.position(), slice.written, compressionName))
          Some(Slice.from(compressedBuffer))
        else
          None
      }
  }

  private[swaydb] case object UnCompressedGroup extends CompressorInternal {
    val compressionName = this.getClass.getSimpleName.dropRight(1)

    override def compress(slice: Slice[Byte]): IO[Option[Slice[Byte]]] = {
      logger.debug(s"Grouped {}.bytes with {}", slice.size, compressionName)
      IO.Success(Some(slice))
    }

    override val minCompressionPercentage: Double = Double.MinValue
  }

  private[swaydb] case class Snappy(minCompressionPercentage: Double) extends CompressorInternal {

    val compressionName = this.getClass.getSimpleName

    override def compress(slice: Slice[Byte]): IO[Option[Slice[Byte]]] =
      //warning check to ensure that compression does not get sliced arrays.
      if (!slice.isOriginalSlice)
        IO.Failure(new Exception(s"Slice is not original or fully. Written: ${slice.written}. Size: ${slice.size}. Array: ${slice.arrayLength}"))
      else
        IO(snappy.Snappy.compress(slice.toArray)) map {
          compressedArray =>
            if (isCompressionSatisfied(minCompressionPercentage, compressedArray.length, slice.size, this.getClass.getSimpleName))
              Some(Slice(compressedArray))
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