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
