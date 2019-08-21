/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.util

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedDeque}

import swaydb.core.io.file.BlockCache
import swaydb.core.queue.MemorySweeper

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object SpeedTest extends App {

  val size = 100000

  val map = new ConcurrentHashMap[Int, Int](size)
  val queue = new ConcurrentLinkedDeque[Int]()

  val blockCache = BlockCache.init(size)

  val limiter = MemorySweeper(size, 10.seconds)

  Benchmark("") {
    (1 to size) foreach {
      i =>

      //        map.put(i, i)
      //        limiter.add(i, blockCache)
      //        queue.add(i)
    }
  }
}