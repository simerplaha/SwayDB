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

import swaydb.Benchmark
import swaydb.config.ForceSave
import swaydb.core.CoreTestData.randomBytesSlice
import swaydb.core.file.sweeper.bytebuffer.ByteBufferCleaner
import swaydb.core.{ACoreSpec, TestCaseSweeper}
import swaydb.effect.Effect
import swaydb.testkit.RunThis._
import swaydb.utils.OperatingSystem
import swaydb.utils.StorageUnits._

import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicBoolean

class ForceSavePerformanceSpec extends ACoreSpec {

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
          var slowest: Double = Double.MinValue
          var quickest: Double = Double.MaxValue

          val bytes = randomBytesSlice(4.mb)

          runThis(100.times, log = true) {
            //create random path and byte slice
            val path = randomFilePath

            val channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
            val buffer = channel.map(MapMode.READ_WRITE, 0, bytes.size)

            buffer.put(bytes.toByteBufferWrap)

            val newTime =
              Benchmark.time(s"Force save memory-mapped file") {
                buffer.force()
              }.toDouble

            slowest = slowest max newTime
            quickest = quickest min newTime

            implicit val forceSaveApplier = ForceSaveApplier.On
            ByteBufferCleaner.initialiseCleaner(buffer, path, new AtomicBoolean(true), ForceSave.Off)
            channel.close()
          }

          println(OperatingSystem.get())
          println("Slowest: " + slowest + " seconds")
          println("fastest: " + quickest + " seconds")

      }
    }

    "FileChannel" in {
      /**
       * Mac
       * Slowest: 0.011574 seconds
       * fastest: 0.005066 seconds
       *
       * Windows
       * Slowest: 0.014361 seconds
       * fastest: 0.008592 seconds
       */

      TestCaseSweeper {
        implicit sweeper =>

          var slowest: Double = Double.MinValue
          var quickest: Double = Double.MaxValue

          val bytes = randomBytesSlice(4.mb)

          runThis(100.times, log = true) {
            //create random path and byte slice
            val path = randomFilePath

            val channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)

            Effect.writeUnclosed(channel, bytes.toByteBufferWrap)

            val newTime =
              Benchmark.time(s"Force save memory-mapped file") {
                channel.force(false)
              }.toDouble

            slowest = slowest max newTime
            quickest = quickest min newTime

            channel.close()
          }

          println(OperatingSystem.get())
          println("Slowest: " + slowest + " seconds")
          println("fastest: " + quickest + " seconds")
      }
    }
  }
}
