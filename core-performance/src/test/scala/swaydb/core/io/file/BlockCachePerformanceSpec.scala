/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.io.file

import swaydb.core.TestData.{randomBytesSlice, randomIntMax}
import swaydb.core.actor.MemorySweeper
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.data.util.StorageUnits._

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
          val file = createFileChannelFileReader(bytes).file.file

          val state = BlockCache.init(MemorySweeper.BlockSweeper(4098.bytes, cacheSize = 1.gb, skipBlockCacheSeekSize = 1.mb, actorConfig = None))

          Benchmark.time("Seek") {
            (1 to 1000000) foreach {
              i =>
                BlockCache.getOrSeek(
                  sourceId = 0,
                  paddingLeft = 0,
                  position = randomIntMax(bytes.size),
                  size = 4098,
                  source = file,
                  state = state
                )
            }
          } should be <= BigDecimal(6.0)

      }
    }
  }
}
