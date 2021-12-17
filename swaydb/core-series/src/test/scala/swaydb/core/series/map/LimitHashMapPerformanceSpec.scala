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

package swaydb.core.series.map

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import swaydb.Benchmark

class LimitHashMapPerformanceSpec extends AnyFlatSpec {

  it should "perform" in {

    val limit = 1000000

    val map = LimitHashMap.volatile[String, Integer](limit, 20)
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
