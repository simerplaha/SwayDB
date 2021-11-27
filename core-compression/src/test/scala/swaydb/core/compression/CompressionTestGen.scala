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

import swaydb.core.compression.CompressionInternal.{LZ4, Snappy, UnCompressed}
import swaydb.config.compression.{DecompressorId, LZ4Instance}

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
        compressor = swaydb.config.compression.LZ4Compressor.random(minCompressionSavingsPercent = minCompressionSavingsPercent)
      )
  }

  implicit class DecompressorImplicits(internal: DecompressorInternal.type) {

    def random(): DecompressorInternal =
      DecompressorInternal(DecompressorId.randomIntId())

    def randomLZ4(): DecompressorInternal.LZ4 =
      DecompressorInternal(DecompressorId.randomLZ4Id())
  }

}
