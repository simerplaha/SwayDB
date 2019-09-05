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

package swaydb.data.api.grouping

import swaydb.data.compression.{LZ4Compressor, LZ4Decompressor, LZ4Instance}

sealed trait Compression
object Compression {
  case class LZ4(compressor: (LZ4Instance, LZ4Compressor), decompressor: (LZ4Instance, LZ4Decompressor)) extends Compression
  case class Snappy(minCompressionPercentage: Double) extends Compression
  case object None extends Compression
}