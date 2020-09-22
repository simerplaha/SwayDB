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

package swaydb.core.util.skiplist

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.util.Benchmark
import swaydb.data.order.KeyOrder

class SliceSkipListSpec extends AnyWordSpec with Matchers {

  "Dsds" in {
    val max = 1000000

    implicit val ordering = KeyOrder(Ordering.Int)
    //        val skipList = SkipList.concurrent[Int, Int, Int, Int](-1, -1)
    val skipList = SkipList.slice[Int, Int, Int, Int](size = max + 1, extendBy = 2, enableHashIndex = false, nullKey = -1, nullValue = -1)

    Benchmark("put") {
      (0 to max) foreach {
        i =>
          skipList.put(i, i)
      }
    }

    //    val shuffle = Random.shuffle((1 to max).toList)

    Benchmark("get") {
      (0 to max) foreach {
        index =>
          skipList.get(index)
        //          println(index + " - " + skipList.get(index))
      }
    }

  }

}
