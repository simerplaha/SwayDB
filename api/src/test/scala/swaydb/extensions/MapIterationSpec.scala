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

import swaydb.api.TestBaseEmbedded
import swaydb.core.IOAssert._
import swaydb.core.RunThis._
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._

class MapIterationSpec0 extends MapIterationSpec {
  val keyValueCount: Int = 1000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.persistent.Map[Int, String](dir = randomDir).assertGet
}

class MapIterationSpec1 extends MapIterationSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.persistent.Map[Int, String](randomDir, mapSize = 1.byte).assertGet
}

class MapIterationSpec2 extends MapIterationSpec {

  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.memory.Map[Int, String](mapSize = 1.byte).assertGet
}

class MapIterationSpec3 extends MapIterationSpec {
  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.memory.Map[Int, String]().assertGet
}

sealed trait MapIterationSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): Map[Int, String]

  "Iteration" should {
    "exclude & include subMap by default" in {
      val db = newDB()

      val firstMap = db.maps.put(1, "rootMap").assertGet
      val secondMap = firstMap.maps.put(2, "first map").assertGet
      val subMap1 = secondMap.maps.put(3, "sub map 1").assertGet
      val subMap2 = secondMap.maps.put(4, "sub map 2").assertGet

      firstMap.stream.toSeq.get shouldBe empty
      firstMap.maps.stream.toSeq.get should contain only ((2, "first map"))

      secondMap.stream.toSeq.get shouldBe empty
      secondMap.maps.stream.toSeq.get should contain only((3, "sub map 1"), (4, "sub map 2"))

      subMap1.stream.toSeq.get shouldBe empty
      subMap2.stream.toSeq.get shouldBe empty

      db.closeDatabase().get
    }
  }

  "Iteration" when {
    "the map contains 1 element" in {
      val db = newDB()

      val firstMap = db.maps.put(1, "rootMap").assertGet
      val secondMap = firstMap.maps.put(2, "first map").assertGet

      firstMap.stream.toSeq.get shouldBe empty
      firstMap.maps.stream.toSeq.get should contain only ((2, "first map"))

      secondMap.put(1, "one").assertGet
      secondMap.size.get shouldBe 1

      secondMap.headOption.assertGet shouldBe ((1, "one"))
      secondMap.lastOption.assertGet shouldBe ((1, "one"))

      secondMap.map(keyValue => (keyValue._1 + 1, keyValue._2)).toSeq.get should contain only ((2, "one"))
      secondMap.foldLeft(List.empty[(Int, String)]) { case (_, keyValue) => List(keyValue) }.get shouldBe List((1, "one"))
      secondMap.reverse.foldLeft(List.empty[(Int, String)]) { case (_, keyValue) => List(keyValue) }.get shouldBe List((1, "one"))
      secondMap.reverse.map(keyValue => (keyValue._1 + 1, keyValue._2)).toSeq.get should contain only ((2, "one"))
      secondMap.reverse.take(100).toSeq.get should contain only ((1, "one"))
      secondMap.reverse.take(1).toSeq.get should contain only ((1, "one"))
      secondMap.take(100).toSeq.get should contain only ((1, "one"))
      secondMap.take(1).toSeq.get should contain only ((1, "one"))
      secondMap.reverse.drop(1).toSeq.get shouldBe empty
      secondMap.drop(1).toSeq.get shouldBe empty
      secondMap.reverse.drop(0).toSeq.get should contain only ((1, "one"))
      secondMap.drop(0).toSeq.get should contain only ((1, "one"))

      db.closeDatabase().get
    }

    "the map contains 2 elements" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap").assertGet
      val firstMap = rootMap.maps.put(2, "first map").assertGet

      firstMap.put(1, "one").assertGet
      firstMap.put(2, "two").assertGet

      firstMap.size.get shouldBe 2
      firstMap.headOption.assertGet shouldBe ((1, "one"))
      firstMap.lastOption.assertGet shouldBe ((2, "two"))

      firstMap.map(keyValue => (keyValue._1 + 1, keyValue._2)).toSeq.get shouldBe List((2, "one"), (3, "two"))
      firstMap.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.get shouldBe List((1, "one"), (2, "two"))
      firstMap.reverse.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.get shouldBe List((2, "two"), (1, "one"))
      firstMap.reverse.map(keyValue => keyValue).toSeq.get shouldBe List((2, "two"), (1, "one"))
      firstMap.reverse.take(100).toSeq.get shouldBe List((2, "two"), (1, "one"))
      firstMap.reverse.take(2).toSeq.get shouldBe List((2, "two"), (1, "one"))
      firstMap.reverse.take(1).toSeq.get should contain only ((2, "two"))
      firstMap.take(100).toSeq.get should contain only((1, "one"), (2, "two"))
      firstMap.take(2).toSeq.get should contain only((1, "one"), (2, "two"))
      firstMap.take(1).toSeq.get should contain only ((1, "one"))
      firstMap.reverse.drop(1).toSeq.get should contain only ((1, "one"))
      firstMap.drop(1).toSeq.get should contain only ((2, "two"))
      firstMap.reverse.drop(0).toSeq.get shouldBe List((2, "two"), (1, "one"))
      firstMap.drop(0).toSeq.get shouldBe List((1, "one"), (2, "two"))

      db.closeDatabase().get
    }

    "Sibling maps" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap1").assertGet

      val subMap1 = rootMap.maps.put(2, "sub map 1").assertGet
      subMap1.put(1, "one").assertGet
      subMap1.put(2, "two").assertGet

      val subMap2 = rootMap.maps.put(3, "sub map 2").assertGet
      subMap2.put(3, "three").assertGet
      subMap2.put(4, "four").assertGet

      rootMap.stream.toSeq.get shouldBe empty
      rootMap.maps.stream.toSeq.get should contain only((2, "sub map 1"), (3, "sub map 2"))

      //FIRST MAP ITERATIONS
      subMap1.size.get shouldBe 2
      subMap1.headOption.assertGet shouldBe ((1, "one"))
      subMap1.lastOption.assertGet shouldBe ((2, "two"))
      subMap1.map(keyValue => (keyValue._1 + 1, keyValue._2)).toSeq.get shouldBe List((2, "one"), (3, "two"))
      subMap1.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.get shouldBe List((1, "one"), (2, "two"))
      subMap1.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous }.get shouldBe List((2, "two"), (1, "one"))
      subMap1.reverse.map(keyValue => keyValue).toSeq.get shouldBe List((2, "two"), (1, "one"))
      subMap1.reverse.take(100).toSeq.get shouldBe List((2, "two"), (1, "one"))
      subMap1.reverse.take(2).toSeq.get shouldBe List((2, "two"), (1, "one"))
      subMap1.reverse.take(1).toSeq.get should contain only ((2, "two"))
      subMap1.take(100).toSeq.get should contain only((1, "one"), (2, "two"))
      subMap1.take(2).toSeq.get should contain only((1, "one"), (2, "two"))
      subMap1.take(1).toSeq.get should contain only ((1, "one"))
      subMap1.reverse.drop(1).toSeq.get should contain only ((1, "one"))
      subMap1.drop(1).toSeq.get should contain only ((2, "two"))
      subMap1.reverse.drop(0).toSeq.get shouldBe List((2, "two"), (1, "one"))
      subMap1.drop(0).toSeq.get shouldBe List((1, "one"), (2, "two"))

      //SECOND MAP ITERATIONS
      subMap2.size.get shouldBe 2
      subMap2.headOption.assertGet shouldBe ((3, "three"))
      subMap2.lastOption.assertGet shouldBe ((4, "four"))
      subMap2.map(keyValue => (keyValue._1, keyValue._2)).toSeq.get shouldBe List((3, "three"), (4, "four"))
      subMap2.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.get shouldBe List((3, "three"), (4, "four"))
      subMap2.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous }.get shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.map(keyValue => keyValue).toSeq.get shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.take(100).toSeq.get shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.take(2).toSeq.get shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.take(1).toSeq.get should contain only ((4, "four"))
      subMap2.take(100).toSeq.get should contain only((3, "three"), (4, "four"))
      subMap2.take(2).toSeq.get should contain only((3, "three"), (4, "four"))
      subMap2.take(1).toSeq.get should contain only ((3, "three"))
      subMap2.reverse.drop(1).toSeq.get should contain only ((3, "three"))
      subMap2.drop(1).toSeq.get should contain only ((4, "four"))
      subMap2.reverse.drop(0).toSeq.get shouldBe List((4, "four"), (3, "three"))
      subMap2.drop(0).toSeq.get shouldBe List((3, "three"), (4, "four"))

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

      rootMap.stream.toSeq.get shouldBe empty
      rootMap.maps.stream.toSeq.get should contain only ((2, "sub map 1"))

      //FIRST MAP ITERATIONS
      subMap1.size.get shouldBe 2
      subMap1.headOption.assertGet shouldBe ((1, "one"))
      subMap1.lastOption.assertGet shouldBe ((2, "two"))
      subMap1.maps.lastOption.assertGet shouldBe ((3, "sub map 2"))
      subMap1.map(keyValue => (keyValue._1, keyValue._2)).toSeq.get shouldBe List((1, "one"), (2, "two"))
      subMap1.maps.map(keyValue => (keyValue._1, keyValue._2)).toSeq.get shouldBe List((3, "sub map 2"))
      subMap1.maps.map(keyValue => (keyValue._1, keyValue._2)).toSeq.get shouldBe List((3, "sub map 2"))
      subMap1.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.get shouldBe List((1, "one"), (2, "two"))
      subMap1.maps.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.get shouldBe List((3, "sub map 2"))
      subMap1.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous }.get shouldBe List((2, "two"), (1, "one"))
      subMap1.maps.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous }.get shouldBe List((3, "sub map 2"))
      subMap1.reverse.map(keyValue => keyValue).toSeq.get shouldBe List((2, "two"), (1, "one"))
      subMap1.maps.reverse.map(keyValue => keyValue).toSeq.get shouldBe List((3, "sub map 2"))
      subMap1.maps.reverse.map(keyValue => keyValue).toSeq.get shouldBe List((3, "sub map 2"))
      subMap1.reverse.take(100).toSeq.get shouldBe List((2, "two"), (1, "one"))
      subMap1.maps.reverse.take(100).toSeq.get shouldBe List((3, "sub map 2"))
      subMap1.maps.reverse.take(100).toSeq.get shouldBe List((3, "sub map 2"))
      subMap1.reverse.take(3).toSeq.get shouldBe List((2, "two"), (1, "one"))
      subMap1.maps.reverse.take(3).toSeq.get shouldBe List((3, "sub map 2"))
      subMap1.reverse.take(1).toSeq.get should contain only ((2, "two"))
      subMap1.maps.reverse.take(1).toSeq.get should contain only ((3, "sub map 2"))
      subMap1.take(100).toSeq.get should contain only((1, "one"), (2, "two"))
      subMap1.maps.take(100).toSeq.get should contain only ((3, "sub map 2"))
      subMap1.take(2).toSeq.get should contain only((1, "one"), (2, "two"))
      subMap1.maps.take(2).toSeq.get should contain only ((3, "sub map 2"))
      subMap1.take(1).toSeq.get should contain only ((1, "one"))
      subMap1.maps.take(1).toSeq.get should contain only ((3, "sub map 2"))
      subMap1.reverse.drop(1).toSeq.get should contain only ((1, "one"))
      subMap1.maps.reverse.drop(1).toSeq.get shouldBe empty
      subMap1.drop(1).toSeq.get should contain only ((2, "two"))
      subMap1.maps.drop(1).toSeq.get shouldBe empty
      subMap1.reverse.drop(0).toSeq.get shouldBe List((2, "two"), (1, "one"))
      subMap1.maps.reverse.drop(0).toSeq.get shouldBe List((3, "sub map 2"))
      subMap1.drop(0).toSeq.get shouldBe List((1, "one"), (2, "two"))
      subMap1.maps.drop(0).toSeq.get shouldBe List((3, "sub map 2"))

      //KEYS ONLY ITERATIONS
      subMap1.keys.size.get shouldBe 2
      subMap1.keys.headOption.assertGet shouldBe 1
      subMap1.keys.lastOption.assertGet shouldBe 2
      //      subMap1.maps.keys.lastOption.assertGet shouldBe 3
      //      subMap1.maps.keys.toSeq.get shouldBe List(3)

      //SECOND MAP ITERATIONS
      subMap2.size.get shouldBe 2
      subMap2.headOption.assertGet shouldBe ((3, "three"))
      subMap2.lastOption.assertGet shouldBe ((4, "four"))
      subMap2.map(keyValue => (keyValue._1, keyValue._2)).toSeq.get shouldBe List((3, "three"), (4, "four"))
      subMap2.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.get shouldBe List((3, "three"), (4, "four"))
      subMap2.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous }.get shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.map(keyValue => keyValue).toSeq.get shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.take(100).toSeq.get shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.take(2).toSeq.get shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.take(1).toSeq.get should contain only ((4, "four"))
      subMap2.take(100).toSeq.get should contain only((3, "three"), (4, "four"))
      subMap2.take(2).toSeq.get should contain only((3, "three"), (4, "four"))
      subMap2.take(1).toSeq.get should contain only ((3, "three"))
      subMap2.reverse.drop(1).toSeq.get should contain only ((3, "three"))
      subMap2.drop(1).toSeq.get should contain only ((4, "four"))
      subMap2.reverse.drop(0).toSeq.get shouldBe List((4, "four"), (3, "three"))
      subMap2.drop(0).toSeq.get shouldBe List((3, "three"), (4, "four"))

      db.closeDatabase().get
    }
  }
}
