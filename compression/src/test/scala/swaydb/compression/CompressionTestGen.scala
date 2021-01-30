/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.compression.CompressionInternal.{LZ4, Snappy, UnCompressed}
import swaydb.data.compression.{DecompressorId, LZ4Instance}

import scala.util.Random

object CompressionTestGen {

  implicit class CompressionImplicits(internal: CompressionInternal.type) {
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

  implicit class CompressorImplicits(internal: CompressorInternal.type) {

    def randomLZ4(minCompressionSavingsPercent: Double = Double.MinValue): CompressorInternal.LZ4 =
      CompressorInternal(
        instance = LZ4Instance.random(),
        compressor = swaydb.data.compression.LZ4Compressor.random(minCompressionSavingsPercent = minCompressionSavingsPercent)
      )
  }

  implicit class DecompressorImplicits(internal: DecompressorInternal.type) {

    def random(): DecompressorInternal =
      DecompressorInternal(DecompressorId.randomIntId())

    def randomLZ4(): DecompressorInternal.LZ4 =
      DecompressorInternal(DecompressorId.randomLZ4Id())
  }

}
