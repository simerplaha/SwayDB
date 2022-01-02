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

package swaydb.core.segment.block.hashindex

import swaydb.core.compression.CoreCompression
import swaydb.config.{HashIndex, UncompressedBlockInfo}
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.FunctionSafe

object HashIndexBlockConfig {

  def disabled(): HashIndexBlockConfig =
    HashIndexBlockConfig(
      maxProbe = -1,
      minimumNumberOfKeys = Int.MaxValue,
      allocateSpace = _ => Int.MinValue,
      minimumNumberOfHits = Int.MaxValue,
      format = HashIndexEntryFormat.Reference,
      ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
      compressions = _ => Seq.empty
    )

  def apply(config: swaydb.config.HashIndex): HashIndexBlockConfig =
    config match {
      case swaydb.config.HashIndex.Off =>
        HashIndexBlockConfig.disabled()

      case enable: swaydb.config.HashIndex.On =>
        HashIndexBlockConfig(
          maxProbe = enable.maxProbe,
          minimumNumberOfKeys = enable.minimumNumberOfKeys,
          minimumNumberOfHits = enable.minimumNumberOfHits,
          format = HashIndexEntryFormat(enable.indexFormat),
          allocateSpace = FunctionSafe.safe(_.requiredSpace, enable.allocateSpace),
          ioStrategy = FunctionSafe.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
          compressions =
            FunctionSafe.safe(
              default = _ => Seq.empty[CoreCompression],
              function = enable.compression(_) map CoreCompression.apply
            )
        )
    }
}

case class HashIndexBlockConfig(maxProbe: Int,
                                minimumNumberOfKeys: Int,
                                minimumNumberOfHits: Int,
                                format: HashIndexEntryFormat,
                                allocateSpace: HashIndex.RequiredSpace => Int,
                                ioStrategy: IOAction => IOStrategy,
                                compressions: UncompressedBlockInfo => Iterable[CoreCompression])
