package swaydb.core.segment.block.hashindex

import swaydb.compression.CompressionInternal
import swaydb.data.config.{HashIndex, UncompressedBlockInfo}
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.FunctionSafe

object HashIndexConfig {

  def disabled(): HashIndexConfig =
    HashIndexConfig(
      maxProbe = -1,
      minimumNumberOfKeys = Int.MaxValue,
      allocateSpace = _ => Int.MinValue,
      minimumNumberOfHits = Int.MaxValue,
      format = HashIndexEntryFormat.Reference,
      ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
      compressions = _ => Seq.empty
    )

  def apply(config: swaydb.data.config.HashIndex): HashIndexConfig =
    config match {
      case swaydb.data.config.HashIndex.Off =>
        HashIndexConfig.disabled()

      case enable: swaydb.data.config.HashIndex.On =>
        HashIndexConfig(
          maxProbe = enable.maxProbe,
          minimumNumberOfKeys = enable.minimumNumberOfKeys,
          minimumNumberOfHits = enable.minimumNumberOfHits,
          format = HashIndexEntryFormat(enable.indexFormat),
          allocateSpace = FunctionSafe.safe(_.requiredSpace, enable.allocateSpace),
          ioStrategy = FunctionSafe.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
          compressions =
            FunctionSafe.safe(
              default = _ => Seq.empty[CompressionInternal],
              function = enable.compression(_) map CompressionInternal.apply
            )
        )
    }
}

case class HashIndexConfig(maxProbe: Int,
                           minimumNumberOfKeys: Int,
                           minimumNumberOfHits: Int,
                           format: HashIndexEntryFormat,
                           allocateSpace: HashIndex.RequiredSpace => Int,
                           ioStrategy: IOAction => IOStrategy,
                           compressions: UncompressedBlockInfo => Iterable[CompressionInternal])
