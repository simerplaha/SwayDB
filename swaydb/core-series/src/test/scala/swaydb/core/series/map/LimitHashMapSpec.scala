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
import swaydb.testkit.TestKit._

class ProbeLimitHashMap extends LimitHashMapSpec {
  def createMap[K, V >: Null](limit: Integer) = LimitHashMap[K, V](limit, 10)
}

class ProbeLimitConcurrentHashMap extends LimitHashMapSpec {
  def createMap[K, V >: Null](limit: Integer) = LimitHashMap.volatile[K, V](limit, 10)
}

class NoProbeLimitHashMap extends LimitHashMapSpec {
  def createMap[K, V >: Null](limit: Integer) =
    eitherOne(
      //create directly with noProbe or via 0 or negative probe.
      LimitHashMap[K, V](limit),
      LimitHashMap[K, V](limit, -eitherOne(0, 1, 2))
    )
}

class NoProbeLimitConcurrentHashMap extends LimitHashMapSpec {
  def createMap[K, V >: Null](limit: Integer) =
    eitherOne(
      //create directly with noProbe or via 0 or negative probe.
      LimitHashMap.volatile[K, V](limit),
      LimitHashMap.volatile[K, V](limit, -eitherOne(0, 1, 2))
    )
}

sealed trait LimitHashMapSpec extends AnyFlatSpec {

  def createMap[K, V >: Null](limit: Integer): LimitHashMap[K, V]

  it should "create empty map for invalid inputs" in {
    Seq(
      createMap[Integer, Integer](0),
      createMap[Integer, Integer](-1),
      createMap[Integer, Integer](Int.MinValue)
    ) foreach {
      map =>
        map.getOrNull(1) should be(null)
        map.put(1, 1)
        map.getOrNull(1) should be(null)
        map.size shouldBe 0
        map.limit shouldBe 0
        map.toList shouldBe empty
    }
  }

  it should "return none for empty map" in {
    val map = createMap[Integer, Integer](1)
    (1 to 100) foreach {
      i =>
        map.getOrNull(i) should be(null)
    }
  }

  it should "write to all slots" in {
    val map = createMap[Integer, Integer](10)

    (1 to 10) foreach {
      i =>
        map.put(i, i)
        map.getOrNull(i) shouldBe i
    }

    (1 to 10) foreach {
      i =>
        map.getOrNull(i) shouldBe i
    }

    map.toList.sorted shouldBe (1 to 10).map(i => (i, i))
  }

  it should "overwrite on overflow" in {

    val map = createMap[Integer, Integer](10)

    (1 to 10) foreach {
      i =>
        map.put(i, i)
    }

    map.toList.sorted shouldBe (1 to 10).map(i => (i, i))

    (11 to 20) foreach {
      i =>
        map.put(i, i)
        map.getOrNull(i) shouldBe i
    }

    (11 to 20) foreach {
      i =>
        map.getOrNull(i) shouldBe i
    }

    map.toList.sorted shouldBe (11 to 20).map(i => (i, i))

    (1 to 10) foreach {
      i =>
        map.getOrNull(i) should be(null)
    }
  }
}
