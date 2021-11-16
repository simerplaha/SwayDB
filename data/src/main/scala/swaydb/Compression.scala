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

package swaydb

import swaydb.data.compression.{LZ4Compressor, LZ4Decompressor, LZ4Instance}
import swaydb.utils.Pair

sealed trait Compression
object Compression {
  def lz4Pair(compressor: Pair[LZ4Instance, LZ4Compressor], decompressor: Pair[LZ4Instance, LZ4Decompressor]): Compression.LZ4 =
    LZ4(
      compressor = compressor.toTuple,
      decompressor = decompressor.toTuple
    )

  case class LZ4(compressor: (LZ4Instance, LZ4Compressor), decompressor: (LZ4Instance, LZ4Decompressor)) extends Compression
  case class Snappy(minCompressionPercentage: Double) extends Compression

  def noneCompression: Compression = Compression.None
  case object None extends Compression
}
