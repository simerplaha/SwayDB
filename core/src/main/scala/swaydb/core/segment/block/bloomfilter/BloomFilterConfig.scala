package swaydb.core.segment.block.bloomfilter

import swaydb.compression.CompressionInternal
import swaydb.data.config.UncompressedBlockInfo
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.FunctionSafe

object BloomFilterConfig {

  def disabled(): BloomFilterConfig =
    BloomFilterConfig(
      falsePositiveRate = 0.0,
      minimumNumberOfKeys = Int.MaxValue,
      optimalMaxProbe = probe => probe,
      ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
      compressions = _ => Seq.empty
    )

  def apply(config: swaydb.data.config.BloomFilter): BloomFilterConfig =
    config match {
      case swaydb.data.config.BloomFilter.Off =>
        BloomFilterConfig(
          falsePositiveRate = 0.0,
          minimumNumberOfKeys = Int.MaxValue,
          optimalMaxProbe = _ => 0,
          ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
          compressions = _ => Seq.empty
        )

      case enable: swaydb.data.config.BloomFilter.On =>
        BloomFilterConfig(
          falsePositiveRate = enable.falsePositiveRate,
          minimumNumberOfKeys = enable.minimumNumberOfKeys,
          optimalMaxProbe = FunctionSafe.safe(probe => probe, enable.updateMaxProbe),
          ioStrategy = FunctionSafe.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
          compressions =
            FunctionSafe.safe(
              default = _ => Seq.empty[CompressionInternal],
              function = enable.compression(_) map CompressionInternal.apply
            )
        )
    }
}

case class BloomFilterConfig(falsePositiveRate: Double,
                             minimumNumberOfKeys: Int,
                             optimalMaxProbe: Int => Int,
                             ioStrategy: IOAction => IOStrategy,
                             compressions: UncompressedBlockInfo => Iterable[CompressionInternal])
