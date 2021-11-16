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

package swaydb.data.config

import swaydb.Compression
import swaydb.data.config.builder.ValuesConfigBuilder
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.Java.JavaFunction

import scala.jdk.CollectionConverters._

object ValuesConfig {

  def builder(): ValuesConfigBuilder.Step0 =
    ValuesConfigBuilder.builder()

}

case class ValuesConfig(compressDuplicateValues: Boolean,
                        compressDuplicateRangeValues: Boolean,
                        blockIOStrategy: IOAction => IOStrategy,
                        compression: UncompressedBlockInfo => Iterable[Compression]) {

  def copyWithCompressDuplicateValues(compressDuplicateValues: Boolean) =
    this.copy(compressDuplicateValues = compressDuplicateValues)

  def copyWithCompressDuplicateRangeValues(compressDuplicateRangeValues: Boolean) =
    this.copy(compressDuplicateRangeValues = compressDuplicateRangeValues)

  def copyWithBlockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) =
    this.copy(blockIOStrategy = blockIOStrategy.apply)

  def copyWithCompression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
    this.copy(compression = info => compression.apply(info).asScala)
}
