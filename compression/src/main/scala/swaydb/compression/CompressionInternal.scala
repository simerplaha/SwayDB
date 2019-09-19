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

import com.typesafe.scalalogging.LazyLogging
import swaydb.Compression

import scala.util.Random

private[swaydb] sealed trait CompressionInternal {

  val compressor: CompressorInternal

  val decompressor: DecompressorInternal
}

private[swaydb] object CompressionInternal extends LazyLogging {

  def apply(compression: Compression): CompressionInternal =
    compression match {
      case lz4: swaydb.Compression.LZ4 =>
        CompressionInternal(lz4)

      case swaydb.Compression.Snappy(minCompressionPercentage) =>
        CompressionInternal.Snappy(minCompressionPercentage)

      case swaydb.Compression.None =>
        CompressionInternal.UnCompressed
    }

  def apply(compression: swaydb.Compression.LZ4): CompressionInternal.LZ4 =
    CompressionInternal.LZ4(
      compressor = CompressorInternal(compression.compressor._1, compression.compressor._2),
      decompressor = DecompressorInternal(compression.decompressor._1, compression.decompressor._2)
    )

  private[swaydb] case class LZ4(compressor: CompressorInternal.LZ4,
                                 decompressor: DecompressorInternal.LZ4) extends CompressionInternal

  private[swaydb] case object UnCompressed extends CompressionInternal {
    val compressor: CompressorInternal = CompressorInternal.UnCompressed
    val decompressor: DecompressorInternal = DecompressorInternal.UnCompressed
  }

  private[swaydb] case class Snappy(minCompressionPercentage: Double) extends CompressionInternal {

    val compressor: CompressorInternal = CompressorInternal.Snappy(minCompressionPercentage)
    val decompressor: DecompressorInternal = DecompressorInternal.Snappy
  }

  def random(minCompressionPercentage: Double = Double.MinValue) =
    if (Random.nextBoolean())
      LZ4(CompressorInternal.randomLZ4(minCompressionSavingsPercent = minCompressionPercentage), DecompressorInternal.randomLZ4())
    else if (Random.nextBoolean())
      Snappy(minCompressionPercentage = minCompressionPercentage)
    else
      UnCompressed

  def randomLZ4OrSnappy(minCompressionPercentage: Double = Double.MinValue) =
    if (Random.nextBoolean())
      randomLZ4(minCompressionPercentage = minCompressionPercentage)
    else
      randomSnappy(minCompressionPercentage = minCompressionPercentage)

  def randomSnappy(minCompressionPercentage: Double = Double.MinValue) =
    Snappy(minCompressionPercentage = minCompressionPercentage)

  def randomLZ4(minCompressionPercentage: Double = Double.MinValue) =
    LZ4(CompressorInternal.randomLZ4(minCompressionSavingsPercent = minCompressionPercentage), DecompressorInternal.randomLZ4())
}