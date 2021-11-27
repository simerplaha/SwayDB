package swaydb.core.segment.block.values

import swaydb.core.compression.CompressionInternal
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
          default = _ => Seq.empty[CompressionInternal],
          function = enable.compression(_) map CompressionInternal.apply
        )
    )
}

case class ValuesBlockConfig(compressDuplicateValues: Boolean,
                             compressDuplicateRangeValues: Boolean,
                             ioStrategy: IOAction => IOStrategy,
                             compressions: UncompressedBlockInfo => Iterable[CompressionInternal])
