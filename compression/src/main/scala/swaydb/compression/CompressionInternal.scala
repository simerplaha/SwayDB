/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.compression

import com.typesafe.scalalogging.LazyLogging
import swaydb.Compression

/**
 * Internal types that have 1 to 1 mapping with the more configurable [[Compression]] type.
 */
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
}
