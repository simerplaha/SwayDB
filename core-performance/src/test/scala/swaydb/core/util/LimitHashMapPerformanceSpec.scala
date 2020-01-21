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
 */

package swaydb.core.util

import java.util.concurrent.ConcurrentHashMap

import org.scalatest.{FlatSpec, Matchers}

class LimitHashMapPerformanceSpec extends FlatSpec with Matchers {

  it should "perform" in {

    val limit = 1000000

    val map = LimitHashMap.concurrent[String, Integer](limit, 20)
    //    val map = new ConcurrentHashMap[String, Integer]()

    Benchmark("") {
      (1 to limit) foreach {
        int =>
          map.put(int.toString, int)
      }
    }

    Benchmark("") {
      println((1 to limit).count(key => map.getOrNull(key.toString) != null))
      //      println("Found: " + (1 to limit).count(key => map.get(key.toString) != null))
    }

    //    println("overwriteCount: " + LimitHashMap.overwriteCount)
  }
}
