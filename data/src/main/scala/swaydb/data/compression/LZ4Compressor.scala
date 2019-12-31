/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.data.compression

import scala.util.Random

sealed trait LZ4Compressor
object LZ4Compressor {

  def random(minCompressionSavingsPercent: Double = Double.MinValue) =
    if (Random.nextBoolean())
      Fast(minCompressionSavingsPercent)
    else
      High(minCompressionSavingsPercent, if (Random.nextBoolean()) None else Some(Math.abs(Random.nextInt(17))))

  case class Fast(minCompressionSavingsPercent: Double) extends LZ4Compressor
  case class High(minCompressionSavingsPercent: Double,
                  compressionLevel: Option[Int] = None) extends LZ4Compressor
}