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

package swaydb.core.segment.block.sortedindex

import swaydb.core.compression.CoreCompression
import swaydb.config.UncompressedBlockInfo
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.FunctionSafe

object SortedIndexBlockConfig {

  def disabled(): SortedIndexBlockConfig =
    SortedIndexBlockConfig(
      ioStrategy = (dataType: IOAction) => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
      enablePrefixCompression = false,
      shouldPrefixCompress = _ => false,
      prefixCompressKeysOnly = false,
      enableAccessPositionIndex = false,
      optimiseForReverseIteration = false,
      normaliseIndex = false,
      compressions = (_: UncompressedBlockInfo) => Seq.empty
    )

  def apply(config: swaydb.config.SortedIndex): SortedIndexBlockConfig =
    config match {
      case config: swaydb.config.SortedIndex.On =>
        apply(config)
    }

  def apply(enable: swaydb.config.SortedIndex.On): SortedIndexBlockConfig =
    SortedIndexBlockConfig(
      ioStrategy = FunctionSafe.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
      shouldPrefixCompress = enable.prefixCompression.shouldCompress,
      prefixCompressKeysOnly = enable.prefixCompression.enabled && enable.prefixCompression.keysOnly,
      enableAccessPositionIndex = enable.enablePositionIndex,
      optimiseForReverseIteration = enable.optimiseForReverseIteration,
      normaliseIndex = enable.prefixCompression.normaliseIndexForBinarySearch,
      enablePrefixCompression = enable.prefixCompression.enabled && !enable.prefixCompression.normaliseIndexForBinarySearch,
      compressions =
        FunctionSafe.safe(
          default = (_: UncompressedBlockInfo) => Iterable.empty[CoreCompression],
          function = enable.compressions(_: UncompressedBlockInfo) map CoreCompression.apply
        )
    )

  def apply(ioStrategy: IOAction => IOStrategy,
            enablePrefixCompression: Boolean,
            shouldPrefixCompress: Int => Boolean,
            prefixCompressKeysOnly: Boolean,
            enableAccessPositionIndex: Boolean,
            optimiseForReverseIteration: Boolean,
            normaliseIndex: Boolean,
            compressions: UncompressedBlockInfo => Iterable[CoreCompression]): SortedIndexBlockConfig =
    new SortedIndexBlockConfig(
      ioStrategy = ioStrategy,
      shouldPrefixCompress = if (normaliseIndex || !enablePrefixCompression) _ => false else shouldPrefixCompress,
      prefixCompressKeysOnly = if (normaliseIndex || !enablePrefixCompression) false else prefixCompressKeysOnly,
      enableAccessPositionIndex = enableAccessPositionIndex,
      optimiseForReverseIteration = !normaliseIndex && optimiseForReverseIteration,
      enablePrefixCompression = !normaliseIndex && enablePrefixCompression,
      normaliseIndex = normaliseIndex,
      compressions = compressions
    )
}

/**
 * Do not create [[SortedIndexBlockConfig]] directly. Use one of the apply functions.
 */
class SortedIndexBlockConfig private(val ioStrategy: IOAction => IOStrategy,
                                     val shouldPrefixCompress: Int => Boolean,
                                     val prefixCompressKeysOnly: Boolean,
                                     val enableAccessPositionIndex: Boolean,
                                     val enablePrefixCompression: Boolean,
                                     val optimiseForReverseIteration: Boolean,
                                     val normaliseIndex: Boolean,
                                     val compressions: UncompressedBlockInfo => Iterable[CoreCompression]) {

  def copy(ioStrategy: IOAction => IOStrategy = ioStrategy,
           shouldPrefixCompress: Int => Boolean = shouldPrefixCompress,
           enableAccessPositionIndex: Boolean = enableAccessPositionIndex,
           normaliseIndex: Boolean = normaliseIndex,
           enablePrefixCompression: Boolean = enablePrefixCompression,
           optimiseForReverseIteration: Boolean = optimiseForReverseIteration,
           compressions: UncompressedBlockInfo => Iterable[CoreCompression] = compressions) =
  //do not use new here. Submit this to the apply function to that rules for creating the config gets applied.
    SortedIndexBlockConfig(
      ioStrategy = ioStrategy,
      shouldPrefixCompress = shouldPrefixCompress,
      enableAccessPositionIndex = enableAccessPositionIndex,
      prefixCompressKeysOnly = prefixCompressKeysOnly,
      enablePrefixCompression = enablePrefixCompression,
      optimiseForReverseIteration = optimiseForReverseIteration,
      normaliseIndex = normaliseIndex,
      compressions = compressions
    )
}
