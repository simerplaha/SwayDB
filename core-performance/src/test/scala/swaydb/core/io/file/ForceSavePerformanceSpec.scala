/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicBoolean
import swaydb.core.TestData.randomBytesSlice
import swaydb.core.sweeper.ByteBufferCleaner
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.data.RunThis._
import swaydb.data.config.ForceSave
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._

class ForceSavePerformanceSpec extends TestBase {

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
