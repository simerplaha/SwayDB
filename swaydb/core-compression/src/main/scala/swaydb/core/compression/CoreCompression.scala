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
private[swaydb] sealed trait CoreCompression {

  val compressor: CoreCompressor

  val decompressor: CoreDecompressor
}

private[swaydb] object CoreCompression extends LazyLogging {

  def apply(compression: Compression): CoreCompression =
    compression match {
      case lz4: swaydb.Compression.LZ4 =>
        CoreCompression(lz4)

      case swaydb.Compression.Snappy(minCompressionPercentage) =>
        CoreCompression.Snappy(minCompressionPercentage)

      case swaydb.Compression.None =>
        CoreCompression.UnCompressed
    }

  def apply(compression: swaydb.Compression.LZ4): CoreCompression.LZ4 =
    CoreCompression.LZ4(
      compressor = CoreCompressor(compression.compressor._1, compression.compressor._2),
      decompressor = CoreDecompressor(compression.decompressor._1, compression.decompressor._2)
    )

  private[swaydb] case class LZ4(compressor: CoreCompressor.LZ4,
                                 decompressor: CoreDecompressor.LZ4) extends CoreCompression

  private[swaydb] case object UnCompressed extends CoreCompression {
    val compressor: CoreCompressor = CoreCompressor.UnCompressed
    val decompressor: CoreDecompressor = CoreDecompressor.UnCompressed
  }

  private[swaydb] case class Snappy(minCompressionPercentage: Double) extends CoreCompression {

    val compressor: CoreCompressor = CoreCompressor.Snappy(minCompressionPercentage)
    val decompressor: CoreDecompressor = CoreDecompressor.Snappy
  }
}
