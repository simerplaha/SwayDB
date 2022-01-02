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

package swaydb.core.segment.block.bloomfilter

import swaydb.core.compression.CoreCompression
import swaydb.config.UncompressedBlockInfo
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.FunctionSafe

object BloomFilterBlockConfig {

  def disabled(): BloomFilterBlockConfig =
    BloomFilterBlockConfig(
      falsePositiveRate = 0.0,
      minimumNumberOfKeys = Int.MaxValue,
      optimalMaxProbe = probe => probe,
      ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
      compressions = _ => Seq.empty
    )

  def apply(config: swaydb.config.BloomFilter): BloomFilterBlockConfig =
    config match {
      case swaydb.config.BloomFilter.Off =>
        BloomFilterBlockConfig(
          falsePositiveRate = 0.0,
          minimumNumberOfKeys = Int.MaxValue,
          optimalMaxProbe = _ => 0,
          ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
          compressions = _ => Seq.empty
        )

      case enable: swaydb.config.BloomFilter.On =>
        BloomFilterBlockConfig(
          falsePositiveRate = enable.falsePositiveRate,
          minimumNumberOfKeys = enable.minimumNumberOfKeys,
          optimalMaxProbe = FunctionSafe.safe(probe => probe, enable.updateMaxProbe),
          ioStrategy = FunctionSafe.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
          compressions =
            FunctionSafe.safe(
              default = _ => Seq.empty[CoreCompression],
              function = enable.compression(_) map CoreCompression.apply
            )
        )
    }
}

case class BloomFilterBlockConfig(falsePositiveRate: Double,
                                  minimumNumberOfKeys: Int,
                                  optimalMaxProbe: Int => Int,
                                  ioStrategy: IOAction => IOStrategy,
                                  compressions: UncompressedBlockInfo => Iterable[CoreCompression])
