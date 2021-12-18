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

import swaydb.core.compression.CoreCompression.{LZ4, Snappy, UnCompressed}
import swaydb.config.compression.{DecompressorId, LZ4Instance}
import swaydb.testkit.TestKit.{eitherOne, randomIntMax}

import scala.util.Random

object CompressionTestKit {

  implicit class CompressionImplicits(internal: CoreCompression.type) {
    def random(minCompressionPercentage: Double = Double.MinValue) =
      if (Random.nextBoolean())
        LZ4(CoreCompressor.randomLZ4(minCompressionSavingsPercent = minCompressionPercentage), CoreDecompressor.randomLZ4())
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
      LZ4(CoreCompressor.randomLZ4(minCompressionSavingsPercent = minCompressionPercentage), CoreDecompressor.randomLZ4())
  }

  implicit class CompressorImplicits(internal: CoreCompressor.type) {

    def randomLZ4(minCompressionSavingsPercent: Double = Double.MinValue): CoreCompressor.LZ4 =
      CoreCompressor(
        instance = LZ4Instance.random(),
        compressor = swaydb.config.compression.LZ4Compressor.random(minCompressionSavingsPercent = minCompressionSavingsPercent)
      )
  }

  implicit class DecompressorImplicits(internal: CoreDecompressor.type) {

    def random(): CoreDecompressor =
      CoreDecompressor(DecompressorId.randomIntId())

    def randomLZ4(): CoreDecompressor.LZ4 =
      CoreDecompressor(DecompressorId.randomLZ4Id())
  }

  def randomCompression(minCompressionPercentage: Double = Double.MinValue): CoreCompression =
    CoreCompression.random(minCompressionPercentage = minCompressionPercentage)

  def randomCompressionLZ4OrSnappy(minCompressionPercentage: Double = Double.MinValue): CoreCompression =
    CoreCompression.randomLZ4OrSnappy(minCompressionPercentage = minCompressionPercentage)

  def randomCompressionSnappy(minCompressionPercentage: Double = Double.MinValue): CoreCompression =
    CoreCompression.randomSnappy(minCompressionPercentage = minCompressionPercentage)

  def randomCompressionLZ4(minCompressionPercentage: Double = Double.MinValue): CoreCompression =
    CoreCompression.randomLZ4(minCompressionPercentage = minCompressionPercentage)

  def randomCompressions(minCompressionPercentage: Double = Double.MinValue): Iterable[CoreCompression] =
    (0 to randomIntMax(3) + 1) map (_ => randomCompression(minCompressionPercentage))

  def randomCompressionsOrEmpty(minCompressionPercentage: Double = Double.MinValue): Iterable[CoreCompression] =
    eitherOne(
      Seq.empty,
      randomCompressions(minCompressionPercentage)
    )

  def randomCompressionsLZ4OrSnappy(minCompressionPercentage: Double = Double.MinValue): Iterable[CoreCompression] =
    (0 to randomIntMax(3) + 1) map (_ => randomCompressionLZ4OrSnappy(minCompressionPercentage))

  def randomCompressionsLZ4OrSnappyOrEmpty(minCompressionPercentage: Double = Double.MinValue): Iterable[CoreCompression] =
    eitherOne(
      Seq.empty,
      randomCompressionsLZ4OrSnappy(minCompressionPercentage)
    )


}
