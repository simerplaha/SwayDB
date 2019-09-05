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

package swaydb.extensions

import org.scalatest.OptionValues._
import swaydb.api.TestBaseEmbedded
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._

class FromSpec0 extends FromSpec {
  val keyValueCount: Int = 1000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.persistent.Map[Int, String](dir = randomDir).right.value.right.value
}

class FromSpec1 extends FromSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.persistent.Map[Int, String](randomDir, mapSize = 1.byte).right.value.right.value
}

class FromSpec2 extends FromSpec {

  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.memory.Map[Int, String](mapSize = 1.byte).right.value.right.value
}

class FromSpec3 extends FromSpec {
  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.memory.Map[Int, String]().right.value.right.value
}

sealed trait FromSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): Map[Int, String]

  "From" should {

    "return empty on an empty Map" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap").right.value
      val firstMap = rootMap.maps.put(2, "first map").right.value

      firstMap
        .from(2)
        .stream
        .materialize
        .right.value shouldBe empty

      db.closeDatabase().right.value
    }

    "if the map contains only 1 element" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap").right.value
      val firstMap = rootMap.maps.put(2, "first map").right.value

      firstMap.put(1, "one").right.value

      firstMap
        .from(2)
        .stream
        .materialize
        .right.value shouldBe empty

      firstMap
        .after(1)
        .stream
        .materialize
        .right.value shouldBe empty

      firstMap
        .from(1)
        .stream
        .materialize
        .right.value should contain only ((1, "one"))

      firstMap
        .fromOrBefore(2)
        .stream
        .materialize
        .right.value should contain only ((1, "one"))

      firstMap
        .fromOrBefore(1)
        .stream
        .materialize
        .right.value should contain only ((1, "one"))

      firstMap
        .after(0)
        .stream
        .materialize
        .right.value should contain only ((1, "one"))

      firstMap
        .fromOrAfter(0)
        .stream
        .materialize
        .right.value should contain only ((1, "one"))

      firstMap
        .stream
        .materialize
        .right.value
        .size shouldBe 1

      firstMap.headOption.right.value should contain((1, "one"))
      firstMap.lastOption.right.value should contain((1, "one"))

      db.closeDatabase().right.value
    }

    "Sibling maps" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap1").right.value

      val subMap1 = rootMap.maps.put(2, "sub map 2").right.value
      subMap1.put(1, "one").right.value
      subMap1.put(2, "two").right.value

      val subMap2 = rootMap.maps.put(3, "sub map three").right.value
      subMap2.put(3, "three").right.value
      subMap2.put(4, "four").right.value

      subMap1.from(3).stream.materialize.runRandomIO.right.value shouldBe empty
      subMap1.after(2).stream.materialize.runRandomIO.right.value shouldBe empty
      subMap1.from(1).stream.materialize.runRandomIO.right.value should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.fromOrBefore(2).stream.materialize.runRandomIO.right.value should contain only ((2, "two"))
      subMap1.fromOrBefore(1).stream.materialize.runRandomIO.right.value should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.after(0).stream.materialize.runRandomIO.right.value should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.fromOrAfter(0).stream.materialize.runRandomIO.right.value should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.size.right.value shouldBe 2
      subMap1.headOption.right.value should contain((1, "one"))
      subMap1.lastOption.right.value should contain((2, "two"))

      subMap2.from(5).stream.materialize.runRandomIO.right.value shouldBe empty
      subMap2.after(4).stream.materialize.runRandomIO.right.value shouldBe empty
      subMap2.from(3).stream.materialize.runRandomIO.right.value should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.fromOrBefore(5).stream.materialize.runRandomIO.right.value should contain only ((4, "four"))
      subMap2.fromOrBefore(3).stream.materialize.runRandomIO.right.value should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.after(0).stream.materialize.runRandomIO.right.value should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.fromOrAfter(1).stream.materialize.runRandomIO.right.value should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.size.right.value shouldBe 2
      subMap2.headOption.right.value.value shouldBe ((3, "three"))
      subMap2.lastOption.right.value.value shouldBe ((4, "four"))

      db.closeDatabase().right.value
    }

    "nested maps" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap1").right.value

      val subMap1 = rootMap.maps.put(2, "sub map 1").right.value
      subMap1.put(1, "one").right.value
      subMap1.put(2, "two").right.value

      val subMap2 = subMap1.maps.put(3, "sub map 2").right.value
      subMap2.put(3, "three").right.value
      subMap2.put(4, "four").right.value

      subMap1.from(4).stream.materialize.runRandomIO.right.value shouldBe empty
      subMap1.after(3).stream.materialize.runRandomIO.right.value shouldBe empty
      subMap1.from(1).stream.materialize.runRandomIO.right.value should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.maps.from(1).stream.materialize.runRandomIO.right.value shouldBe empty
      subMap1.maps.after(1).stream.materialize.runRandomIO.right.value should contain only ((3, "sub map 2"))
      subMap1.fromOrBefore(2).stream.materialize.runRandomIO.right.value should contain only ((2, "two"))
      subMap1.maps.fromOrBefore(2).stream.materialize.runRandomIO.right.value should contain only ((3, "sub map 2"))

      subMap1.fromOrBefore(1).stream.materialize.runRandomIO.right.value should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.maps.fromOrBefore(1).stream.materialize.runRandomIO.right.value should contain only ((3, "sub map 2"))
      subMap1.after(0).stream.materialize.runRandomIO.right.value should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.maps.after(0).stream.materialize.runRandomIO.right.value should contain only ((3, "sub map 2"))
      subMap1.fromOrAfter(0).stream.materialize.runRandomIO.right.value should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.maps.fromOrAfter(0).stream.materialize.runRandomIO.right.value should contain only ((3, "sub map 2"))
      subMap1.size.right.value shouldBe 2
      subMap1.headOption.right.value.value shouldBe ((1, "one"))
      subMap1.maps.lastOption.right.value.value shouldBe ((3, "sub map 2"))

      subMap2.from(5).stream.materialize.runRandomIO.right.value shouldBe empty
      subMap2.after(4).stream.materialize.runRandomIO.right.value shouldBe empty
      subMap2.from(3).stream.materialize.runRandomIO.right.value should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.fromOrBefore(5).stream.materialize.runRandomIO.right.value should contain only ((4, "four"))
      subMap2.fromOrBefore(3).stream.materialize.runRandomIO.right.value should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.after(0).stream.materialize.runRandomIO.right.value should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.fromOrAfter(1).stream.materialize.runRandomIO.right.value should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.size.right.value shouldBe 2
      subMap2.headOption.right.value.value shouldBe ((3, "three"))
      subMap2.lastOption.right.value.value shouldBe ((4, "four"))

      db.closeDatabase().right.value
    }
  }
}
