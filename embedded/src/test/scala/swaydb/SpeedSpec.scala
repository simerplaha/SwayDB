/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb

import swaydb.configs.level.{MemoryConfig, PersistentConfig}
import swaydb.core.util.Benchmark
import swaydb.core.{CoreAPI, TestBase}
import swaydb.data.accelerate.Accelerator
import swaydb.data.config.{MMAP, RecoveryMode}
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._

import scala.concurrent.duration._

class SpeedSpec extends TestBase with Benchmark {

  //  override def deleteFiles: Boolean = false

  "Speed spec" should {

    "Speed" in {

      val keys = (0L to 1000000L) map LongSerializer.write
      val value = Slice(randomBytes(60))

      import KeyOrder.default

      val db =
        new SwayDB(
          CoreAPI(
            config = PersistentConfig(
              dir = testDir,
              otherDirs = Seq.empty,
              mapSize = 4.mb,
              segmentSize = 2.mb,
              recoveryMode = RecoveryMode.Report,
              appendixFlushCheckpointSize = 4.mb,
              mmapMaps = true,
              mmapSegments = MMAP.WriteAndRead,
              mmapAppendix = true,
              bloomFilterFalsePositiveRate = 0.1,
              acceleration = Accelerator.noBrakes()
            ),
            maxOpenSegments = 1000,
            cacheSize = 10.mb,
            cacheCheckDelay = 5.seconds,
            segmentsOpenCheckDelay = 5.seconds
          ).assertGet
        )

      //      val db =
      //        new SwayDB(
      //          CoreAPI(
      //            config = MemoryConfig(
      //              mapSize = 4.mb,
      //              segmentSize = 2.mb,
      //              bloomFilterFalsePositiveRate = 0.1,
      //              acceleration = Accelerator.cruise
      //            ),
      //            maxOpenSegments = 1000,
      //            cacheSize = 1.gb,
      //            cacheCheckDelay = 10.seconds,
      //            segmentsOpenCheckDelay = 10.seconds
      //          ).assertGet
      //        )

      benchmark("Write benchmark") {
        keys foreach {
          key =>
            db.put(key, value)
        }
      }

      benchmark("Read benchmark 1") {
        keys foreach {
          key =>
            //            println(key)
            db.get(key)
        }
      }

      sleep(10.seconds)
      benchmark("Read benchmark 2") {
        keys foreach {
          key =>
            db.get(key)
        }
      }
      //      sleep(10.minutes)
    }
  }
}
