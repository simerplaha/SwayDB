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

package swaydb.core.segment.block.values

import swaydb.core.compression.CoreCompression
import swaydb.config.UncompressedBlockInfo
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.FunctionSafe

object ValuesBlockConfig {

  def disabled(): ValuesBlockConfig =
    ValuesBlockConfig(
      compressDuplicateValues = false,
      compressDuplicateRangeValues = false,
      ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
      compressions = _ => Seq.empty
    )

  def apply(enable: swaydb.config.ValuesConfig): ValuesBlockConfig =
    ValuesBlockConfig(
      compressDuplicateValues = enable.compressDuplicateValues,
      compressDuplicateRangeValues = enable.compressDuplicateRangeValues,
      ioStrategy = FunctionSafe.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
      compressions =
        FunctionSafe.safe(
          default = _ => Seq.empty[CoreCompression],
          function = enable.compression(_) map CoreCompression.apply
        )
    )
}

case class ValuesBlockConfig(compressDuplicateValues: Boolean,
                             compressDuplicateRangeValues: Boolean,
                             ioStrategy: IOAction => IOStrategy,
                             compressions: UncompressedBlockInfo => Iterable[CoreCompression])
