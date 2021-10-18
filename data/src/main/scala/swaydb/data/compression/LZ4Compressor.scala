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
