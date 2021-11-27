package swaydb.core.segment.block.sortedindex

import swaydb.core.compression.CompressionInternal
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
          default = (_: UncompressedBlockInfo) => Iterable.empty[CompressionInternal],
          function = enable.compressions(_: UncompressedBlockInfo) map CompressionInternal.apply
        )
    )

  def apply(ioStrategy: IOAction => IOStrategy,
            enablePrefixCompression: Boolean,
            shouldPrefixCompress: Int => Boolean,
            prefixCompressKeysOnly: Boolean,
            enableAccessPositionIndex: Boolean,
            optimiseForReverseIteration: Boolean,
            normaliseIndex: Boolean,
            compressions: UncompressedBlockInfo => Iterable[CompressionInternal]): SortedIndexBlockConfig =
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
                                     val compressions: UncompressedBlockInfo => Iterable[CompressionInternal]) {

  def copy(ioStrategy: IOAction => IOStrategy = ioStrategy,
           shouldPrefixCompress: Int => Boolean = shouldPrefixCompress,
           enableAccessPositionIndex: Boolean = enableAccessPositionIndex,
           normaliseIndex: Boolean = normaliseIndex,
           enablePrefixCompression: Boolean = enablePrefixCompression,
           optimiseForReverseIteration: Boolean = optimiseForReverseIteration,
           compressions: UncompressedBlockInfo => Iterable[CompressionInternal] = compressions) =
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
