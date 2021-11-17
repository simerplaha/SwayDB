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

package swaydb.core.file

import swaydb.core.TestData._
import swaydb.core.segment.block.BlockCache
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.Benchmark
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.utils.StorageUnits._

class BlockCachePerformanceSpec extends TestBase {

  "force save benchmark" when {
    "memory-mapped file" in {

      /**
       * Mac
       * Slowest: 0.027715 seconds
       * fastest: 0.005657 seconds
       *
       * Windows
       * Slowest: 0.014765 seconds
       * fastest: 0.009364 seconds
       */

      TestCaseSweeper {
        implicit sweeper =>
          val bytes = Benchmark("Generating bytes")(randomBytesSlice(1.gb))
          val file = createStandardFileFileReader(bytes).file

          val state = BlockCache.forSearch(bytes.size, Some(MemorySweeper.BlockSweeper(4098.bytes, cacheSize = 1.gb, skipBlockCacheSeekSize = 1.mb, false, actorConfig = None))).get

          Benchmark.time("Seek") {
            (1 to 1000000) foreach {
              i =>
                BlockCache.getOrSeek(
                  position = randomIntMax(bytes.size),
                  size = 4098,
                  source = file.toBlockCacheSource,
                  state = state
                )
            }
          } should be <= BigDecimal(6.0)

      }
    }
  }
}
