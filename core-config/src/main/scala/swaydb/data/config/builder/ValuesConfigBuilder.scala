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

package swaydb.data.config.builder

import swaydb.Compression
import swaydb.data.config.{UncompressedBlockInfo, ValuesConfig}
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.Java.JavaFunction

import scala.jdk.CollectionConverters._

class ValuesConfigBuilder {
  private var compressDuplicateValues: Boolean = _
  private var compressDuplicateRangeValues: Boolean = _
  private var blockIOStrategy: JavaFunction[IOAction, IOStrategy] = _
}

object ValuesConfigBuilder {

  class Step0(builder: ValuesConfigBuilder) {
    def compressDuplicateValues(compressDuplicateValues: Boolean) = {
      builder.compressDuplicateValues = compressDuplicateValues
      new Step1(builder)
    }
  }

  class Step1(builder: ValuesConfigBuilder) {
    def compressDuplicateRangeValues(compressDuplicateRangeValues: Boolean) = {
      builder.compressDuplicateRangeValues = compressDuplicateRangeValues
      new Step2(builder)
    }
  }

  class Step2(builder: ValuesConfigBuilder) {
    def blockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) = {
      builder.blockIOStrategy = blockIOStrategy
      new Step3(builder)
    }
  }

  class Step3(builder: ValuesConfigBuilder) {
    def compression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      ValuesConfig(
        compressDuplicateValues = builder.compressDuplicateValues,
        compressDuplicateRangeValues = builder.compressDuplicateRangeValues,
        blockIOStrategy = builder.blockIOStrategy.apply,
        compression = compression.apply(_).asScala
      )
  }

  def builder() = new Step0(new ValuesConfigBuilder())
}
