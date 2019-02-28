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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.extension

import swaydb.core.TestBase
import swaydb.serializers.Default._
import swaydb.{SwayDB, TestBaseEmbedded}
import swaydb.data.util.StorageUnits._
import scala.concurrent.duration._
import swaydb.core.IOAssert._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._

class FromSpec0 extends FromSpec {
  val keyValueCount: Int = 1000

  override def newDB(): Map[Int, String] =
    swaydb.persistent.Map[Key[Int], Option[String]](dir = randomDir).assertGet.extend.assertGet
}

class FromSpec1 extends FromSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String] =
    swaydb.persistent.Map[Key[Int], Option[String]](randomDir, mapSize = 1.byte).assertGet.extend.assertGet
}

class FromSpec2 extends FromSpec {

  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String] =
    swaydb.memory.Map[Key[Int], Option[String]](mapSize = 1.byte).assertGet.extend.assertGet
}

class FromSpec3 extends FromSpec {
  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String] =
    swaydb.memory.Map[Key[Int], Option[String]]().assertGet.extend.assertGet
}

sealed trait FromSpec extends TestBase with TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): Map[Int, String]

  "From" should {

    "return empty on an empty Map" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap").assertGet
      val firstMap = rootMap.maps.put(2, "first map").assertGet

      firstMap
        .from(2)
        .toList shouldBe empty

      db.closeDatabase().get

    }

    "if the map contains only 1 element" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap").assertGet
      val firstMap = rootMap.maps.put(2, "first map").assertGet

      firstMap.put(1, "one").assertGet

      firstMap
        .from(2)
        .toList shouldBe empty

      firstMap
        .after(1)
        .toList shouldBe empty

      firstMap
        .from(1)
        .toList should contain only ((1, "one"))

      firstMap
        .fromOrBefore(2)
        .toList should contain only ((1, "one"))

      firstMap
        .fromOrBefore(1)
        .toList should contain only ((1, "one"))

      firstMap
        .after(0)
        .toList should contain only ((1, "one"))

      firstMap
        .fromOrAfter(0)
        .toList should contain only ((1, "one"))

      firstMap.size shouldBe 1

      firstMap.head shouldBe ((1, "one"))
      firstMap.last shouldBe ((1, "one"))

      db.closeDatabase().get
    }

    "Sibling maps" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap1").assertGet

      val subMap1 = rootMap.maps.put(2, "sub map 2").assertGet
      subMap1.put(1, "one").assertGet
      subMap1.put(2, "two").assertGet

      val subMap2 = rootMap.maps.put(3, "sub map three").assertGet
      subMap2.put(3, "three").assertGet
      subMap2.put(4, "four").assertGet

      subMap1.from(3).toList shouldBe empty
      subMap1.after(2).toList shouldBe empty
      subMap1.from(1).toList should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.fromOrBefore(2).toList should contain only ((2, "two"))
      subMap1.fromOrBefore(1).toList should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.after(0).toList should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.fromOrAfter(0).toList should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.size shouldBe 2
      subMap1.head shouldBe ((1, "one"))
      subMap1.last shouldBe ((2, "two"))

      subMap2.from(5).toList shouldBe empty
      subMap2.after(4).toList shouldBe empty
      subMap2.from(3).toList should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.fromOrBefore(5).toList should contain only ((4, "four"))
      subMap2.fromOrBefore(3).toList should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.after(0).toList should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.fromOrAfter(1).toList should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.size shouldBe 2
      subMap2.head shouldBe ((3, "three"))
      subMap2.last shouldBe ((4, "four"))

      db.closeDatabase().get
    }

    "nested maps" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap1").assertGet

      val subMap1 = rootMap.maps.put(2, "sub map 1").assertGet
      subMap1.put(1, "one").assertGet
      subMap1.put(2, "two").assertGet

      val subMap2 = subMap1.maps.put(3, "sub map 2").assertGet
      subMap2.put(3, "three").assertGet
      subMap2.put(4, "four").assertGet

      subMap1.from(4).toList shouldBe empty
      subMap1.after(3).toList shouldBe empty
      subMap1.from(1).toList should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.maps.from(1).toList shouldBe empty
      subMap1.maps.after(1).toList should contain only ((3, "sub map 2"))
      subMap1.fromOrBefore(2).toList should contain only ((2, "two"))
      subMap1.maps.fromOrBefore(2).toList should contain only ((3, "sub map 2"))

      subMap1.fromOrBefore(1).toList should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.maps.fromOrBefore(1).toList should contain only ((3, "sub map 2"))
      subMap1.after(0).toList should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.maps.after(0).toList should contain only ((3, "sub map 2"))
      subMap1.fromOrAfter(0).toList should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.maps.fromOrAfter(0).toList should contain only ((3, "sub map 2"))
      subMap1.size shouldBe 2
      subMap1.head shouldBe ((1, "one"))
      subMap1.maps.last shouldBe ((3, "sub map 2"))

      subMap2.from(5).toList shouldBe empty
      subMap2.after(4).toList shouldBe empty
      subMap2.from(3).toList should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.fromOrBefore(5).toList should contain only ((4, "four"))
      subMap2.fromOrBefore(3).toList should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.after(0).toList should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.fromOrAfter(1).toList should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.size shouldBe 2
      subMap2.head shouldBe ((3, "three"))
      subMap2.last shouldBe ((4, "four"))

      db.closeDatabase().get
    }
  }
}
