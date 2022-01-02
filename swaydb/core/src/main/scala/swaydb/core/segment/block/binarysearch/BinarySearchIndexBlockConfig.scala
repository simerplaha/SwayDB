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

package swaydb.core.segment.block.binarysearch

import swaydb.core.compression.CoreCompression
import swaydb.config.UncompressedBlockInfo
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.FunctionSafe

object BinarySearchIndexBlockConfig {

  def disabled(): BinarySearchIndexBlockConfig =
    BinarySearchIndexBlockConfig(
      enabled = false,
      format = BinarySearchEntryFormat.Reference,
      minimumNumberOfKeys = 0,
      fullIndex = false,
      searchSortedIndexDirectlyIfPossible = true,
      ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
      compressions = _ => Seq.empty
    )

  def apply(config: swaydb.config.BinarySearchIndex): BinarySearchIndexBlockConfig =
    config match {
      case swaydb.config.BinarySearchIndex.Off(searchSortedIndexDirectly) =>
        BinarySearchIndexBlockConfig(
          enabled = false,
          format = BinarySearchEntryFormat.Reference,
          minimumNumberOfKeys = Int.MaxValue,
          fullIndex = false,
          searchSortedIndexDirectlyIfPossible = searchSortedIndexDirectly,
          ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
          compressions = _ => Seq.empty
        )

      case enable: swaydb.config.BinarySearchIndex.FullIndex =>
        BinarySearchIndexBlockConfig(
          enabled = true,
          format = BinarySearchEntryFormat(enable.indexFormat),
          minimumNumberOfKeys = enable.minimumNumberOfKeys,
          searchSortedIndexDirectlyIfPossible = enable.searchSortedIndexDirectly,
          fullIndex = true,
          ioStrategy = FunctionSafe.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
          compressions =
            FunctionSafe.safe(
              default = _ => Seq.empty[CoreCompression],
              function = enable.compression(_) map CoreCompression.apply
            )
        )

      case enable: swaydb.config.BinarySearchIndex.SecondaryIndex =>
        BinarySearchIndexBlockConfig(
          enabled = true,
          format = BinarySearchEntryFormat(enable.indexFormat),
          minimumNumberOfKeys = enable.minimumNumberOfKeys,
          searchSortedIndexDirectlyIfPossible = enable.searchSortedIndexDirectlyIfPreNormalised,
          fullIndex = false,
          ioStrategy = FunctionSafe.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
          compressions =
            FunctionSafe.safe(
              default = _ => Seq.empty[CoreCompression],
              function = enable.compression(_) map CoreCompression.apply
            )
        )
    }
}

case class BinarySearchIndexBlockConfig(enabled: Boolean,
                                        format: BinarySearchEntryFormat,
                                        minimumNumberOfKeys: Int,
                                        searchSortedIndexDirectlyIfPossible: Boolean,
                                        fullIndex: Boolean,
                                        ioStrategy: IOAction => IOStrategy,
                                        compressions: UncompressedBlockInfo => Iterable[CoreCompression])
