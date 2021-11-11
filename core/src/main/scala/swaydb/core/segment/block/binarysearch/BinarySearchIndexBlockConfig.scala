package swaydb.core.segment.block.binarysearch

import swaydb.compression.CompressionInternal
import swaydb.data.config.UncompressedBlockInfo
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

  def apply(config: swaydb.data.config.BinarySearchIndex): BinarySearchIndexBlockConfig =
    config match {
      case swaydb.data.config.BinarySearchIndex.Off(searchSortedIndexDirectly) =>
        BinarySearchIndexBlockConfig(
          enabled = false,
          format = BinarySearchEntryFormat.Reference,
          minimumNumberOfKeys = Int.MaxValue,
          fullIndex = false,
          searchSortedIndexDirectlyIfPossible = searchSortedIndexDirectly,
          ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
          compressions = _ => Seq.empty
        )

      case enable: swaydb.data.config.BinarySearchIndex.FullIndex =>
        BinarySearchIndexBlockConfig(
          enabled = true,
          format = BinarySearchEntryFormat(enable.indexFormat),
          minimumNumberOfKeys = enable.minimumNumberOfKeys,
          searchSortedIndexDirectlyIfPossible = enable.searchSortedIndexDirectly,
          fullIndex = true,
          ioStrategy = FunctionSafe.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
          compressions =
            FunctionSafe.safe(
              default = _ => Seq.empty[CompressionInternal],
              function = enable.compression(_) map CompressionInternal.apply
            )
        )

      case enable: swaydb.data.config.BinarySearchIndex.SecondaryIndex =>
        BinarySearchIndexBlockConfig(
          enabled = true,
          format = BinarySearchEntryFormat(enable.indexFormat),
          minimumNumberOfKeys = enable.minimumNumberOfKeys,
          searchSortedIndexDirectlyIfPossible = enable.searchSortedIndexDirectlyIfPreNormalised,
          fullIndex = false,
          ioStrategy = FunctionSafe.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
          compressions =
            FunctionSafe.safe(
              default = _ => Seq.empty[CompressionInternal],
              function = enable.compression(_) map CompressionInternal.apply
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
                                        compressions: UncompressedBlockInfo => Iterable[CompressionInternal])
