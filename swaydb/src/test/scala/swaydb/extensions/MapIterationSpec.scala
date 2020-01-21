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

package swaydb.extensions

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.api.TestBaseEmbedded
import swaydb.core.RunThis._
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._

class MapIterationSpec0 extends MapIterationSpec {
  val keyValueCount: Int = 1000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.persistent.Map[Int, String, Nothing](dir = randomDir).right.value.right.value
}

class MapIterationSpec1 extends MapIterationSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.persistent.Map[Int, String, Nothing](randomDir, mapSize = 1.byte).right.value.right.value
}

class MapIterationSpec2 extends MapIterationSpec {

  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.memory.Map[Int, String, Nothing](mapSize = 1.byte).right.value.right.value
}

class MapIterationSpec3 extends MapIterationSpec {
  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.memory.Map[Int, String, Nothing]().right.value.right.value
}

sealed trait MapIterationSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): Map[Int, String]

  "Iteration" should {
    "exclude & include subMap by default" in {
      val db = newDB()

      val firstMap = db.maps.put(1, "rootMap").right.value
      val secondMap = firstMap.maps.put(2, "first map").right.value
      val subMap1 = secondMap.maps.put(3, "sub map 1").right.value
      val subMap2 = secondMap.maps.put(4, "sub map 2").right.value

      firstMap.stream.materialize.runRandomIO.right.value shouldBe empty
      firstMap.maps.stream.materialize.runRandomIO.right.value should contain only ((2, "first map"))

      secondMap.stream.materialize.runRandomIO.right.value shouldBe empty
      secondMap.maps.stream.materialize.runRandomIO.right.value should contain only((3, "sub map 1"), (4, "sub map 2"))

      subMap1.stream.materialize.runRandomIO.right.value shouldBe empty
      subMap2.stream.materialize.runRandomIO.right.value shouldBe empty

      db.closeDatabase().right.value
    }
  }

  "Iteration" when {
    "the map contains 1 element" in {
      val db = newDB()

      val firstMap = db.maps.put(1, "rootMap").right.value
      val secondMap = firstMap.maps.put(2, "first map").right.value

      firstMap.stream.materialize.runRandomIO.right.value shouldBe empty
      firstMap.maps.stream.materialize.runRandomIO.right.value should contain only ((2, "first map"))

      secondMap.put(1, "one").right.value
      secondMap.size.right.value shouldBe 1

      secondMap.headOption.right.value.value shouldBe ((1, "one"))
      secondMap.lastOption.right.value.value shouldBe ((1, "one"))

      secondMap.map(keyValue => (keyValue._1 + 1, keyValue._2)).materialize.runRandomIO.right.value should contain only ((2, "one"))
      secondMap.foldLeft(List.empty[(Int, String)]) { case (_, keyValue) => List(keyValue) }.right.value shouldBe List((1, "one"))
      secondMap.reverse.foldLeft(List.empty[(Int, String)]) { case (_, keyValue) => List(keyValue) }.right.value shouldBe List((1, "one"))
      secondMap.reverse.map(keyValue => (keyValue._1 + 1, keyValue._2)).materialize.runRandomIO.right.value should contain only ((2, "one"))
      secondMap.reverse.take(100).materialize.runRandomIO.right.value should contain only ((1, "one"))
      secondMap.reverse.take(1).materialize.runRandomIO.right.value should contain only ((1, "one"))
      secondMap.take(100).materialize.runRandomIO.right.value should contain only ((1, "one"))
      secondMap.take(1).materialize.runRandomIO.right.value should contain only ((1, "one"))
      secondMap.reverse.drop(1).materialize.runRandomIO.right.value shouldBe empty
      secondMap.drop(1).materialize.runRandomIO.right.value shouldBe empty
      secondMap.reverse.drop(0).materialize.runRandomIO.right.value should contain only ((1, "one"))
      secondMap.drop(0).materialize.runRandomIO.right.value should contain only ((1, "one"))

      db.closeDatabase().right.value
    }

    "the map contains 2 elements" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap").right.value
      val firstMap = rootMap.maps.put(2, "first map").right.value

      firstMap.put(1, "one").right.value
      firstMap.put(2, "two").right.value

      firstMap.size.right.value shouldBe 2
      firstMap.headOption.right.value.value shouldBe ((1, "one"))
      firstMap.lastOption.right.value.value shouldBe ((2, "two"))

      firstMap.map(keyValue => (keyValue._1 + 1, keyValue._2)).materialize.runRandomIO.right.value shouldBe List((2, "one"), (3, "two"))
      firstMap.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.right.value shouldBe List((1, "one"), (2, "two"))
      firstMap.reverse.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.right.value shouldBe List((2, "two"), (1, "one"))
      firstMap.reverse.map(keyValue => keyValue).materialize.runRandomIO.right.value shouldBe List((2, "two"), (1, "one"))
      firstMap.reverse.take(100).materialize.runRandomIO.right.value shouldBe List((2, "two"), (1, "one"))
      firstMap.reverse.take(2).materialize.runRandomIO.right.value shouldBe List((2, "two"), (1, "one"))
      firstMap.reverse.take(1).materialize.runRandomIO.right.value should contain only ((2, "two"))
      firstMap.take(100).materialize.runRandomIO.right.value should contain only((1, "one"), (2, "two"))
      firstMap.take(2).materialize.runRandomIO.right.value should contain only((1, "one"), (2, "two"))
      firstMap.take(1).materialize.runRandomIO.right.value should contain only ((1, "one"))
      firstMap.reverse.drop(1).materialize.runRandomIO.right.value should contain only ((1, "one"))
      firstMap.drop(1).materialize.runRandomIO.right.value should contain only ((2, "two"))
      firstMap.reverse.drop(0).materialize.runRandomIO.right.value shouldBe List((2, "two"), (1, "one"))
      firstMap.drop(0).materialize.runRandomIO.right.value shouldBe List((1, "one"), (2, "two"))

      db.closeDatabase().right.value
    }

    "Sibling maps" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap1").right.value

      val subMap1 = rootMap.maps.put(2, "sub map 1").right.value
      subMap1.put(1, "one").right.value
      subMap1.put(2, "two").right.value

      val subMap2 = rootMap.maps.put(3, "sub map 2").right.value
      subMap2.put(3, "three").right.value
      subMap2.put(4, "four").right.value

      rootMap.stream.materialize.runRandomIO.right.value shouldBe empty
      rootMap.maps.stream.materialize.runRandomIO.right.value should contain only((2, "sub map 1"), (3, "sub map 2"))

      //FIRST MAP ITERATIONS
      subMap1.size.right.value shouldBe 2
      subMap1.headOption.right.value.value shouldBe ((1, "one"))
      subMap1.lastOption.right.value.value shouldBe ((2, "two"))
      subMap1.map(keyValue => (keyValue._1 + 1, keyValue._2)).materialize.runRandomIO.right.value shouldBe List((2, "one"), (3, "two"))
      subMap1.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.right.value shouldBe List((1, "one"), (2, "two"))
      subMap1.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous }.right.value shouldBe List((2, "two"), (1, "one"))
      subMap1.reverse.map(keyValue => keyValue).materialize.runRandomIO.right.value shouldBe List((2, "two"), (1, "one"))
      subMap1.reverse.take(100).materialize.runRandomIO.right.value shouldBe List((2, "two"), (1, "one"))
      subMap1.reverse.take(2).materialize.runRandomIO.right.value shouldBe List((2, "two"), (1, "one"))
      subMap1.reverse.take(1).materialize.runRandomIO.right.value should contain only ((2, "two"))
      subMap1.take(100).materialize.runRandomIO.right.value should contain only((1, "one"), (2, "two"))
      subMap1.take(2).materialize.runRandomIO.right.value should contain only((1, "one"), (2, "two"))
      subMap1.take(1).materialize.runRandomIO.right.value should contain only ((1, "one"))
      subMap1.reverse.drop(1).materialize.runRandomIO.right.value should contain only ((1, "one"))
      subMap1.drop(1).materialize.runRandomIO.right.value should contain only ((2, "two"))
      subMap1.reverse.drop(0).materialize.runRandomIO.right.value shouldBe List((2, "two"), (1, "one"))
      subMap1.drop(0).materialize.runRandomIO.right.value shouldBe List((1, "one"), (2, "two"))

      //SECOND MAP ITERATIONS
      subMap2.size.right.value shouldBe 2
      subMap2.headOption.right.value.value shouldBe ((3, "three"))
      subMap2.lastOption.right.value.value shouldBe ((4, "four"))
      subMap2.map(keyValue => (keyValue._1, keyValue._2)).materialize.runRandomIO.right.value shouldBe List((3, "three"), (4, "four"))
      subMap2.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.right.value shouldBe List((3, "three"), (4, "four"))
      subMap2.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous }.right.value shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.map(keyValue => keyValue).materialize.runRandomIO.right.value shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.take(100).materialize.runRandomIO.right.value shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.take(2).materialize.runRandomIO.right.value shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.take(1).materialize.runRandomIO.right.value should contain only ((4, "four"))
      subMap2.take(100).materialize.runRandomIO.right.value should contain only((3, "three"), (4, "four"))
      subMap2.take(2).materialize.runRandomIO.right.value should contain only((3, "three"), (4, "four"))
      subMap2.take(1).materialize.runRandomIO.right.value should contain only ((3, "three"))
      subMap2.reverse.drop(1).materialize.runRandomIO.right.value should contain only ((3, "three"))
      subMap2.drop(1).materialize.runRandomIO.right.value should contain only ((4, "four"))
      subMap2.reverse.drop(0).materialize.runRandomIO.right.value shouldBe List((4, "four"), (3, "three"))
      subMap2.drop(0).materialize.runRandomIO.right.value shouldBe List((3, "three"), (4, "four"))

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

      rootMap.stream.materialize.runRandomIO.right.value shouldBe empty
      rootMap.maps.stream.materialize.runRandomIO.right.value should contain only ((2, "sub map 1"))

      //FIRST MAP ITERATIONS
      subMap1.size.right.value shouldBe 2
      subMap1.headOption.right.value.value shouldBe ((1, "one"))
      subMap1.lastOption.right.value.value shouldBe ((2, "two"))
      subMap1.maps.lastOption.right.value.value shouldBe ((3, "sub map 2"))
      subMap1.map(keyValue => (keyValue._1, keyValue._2)).materialize.runRandomIO.right.value shouldBe List((1, "one"), (2, "two"))
      subMap1.maps.map(keyValue => (keyValue._1, keyValue._2)).materialize.runRandomIO.right.value shouldBe List((3, "sub map 2"))
      subMap1.maps.map(keyValue => (keyValue._1, keyValue._2)).materialize.runRandomIO.right.value shouldBe List((3, "sub map 2"))
      subMap1.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.right.value shouldBe List((1, "one"), (2, "two"))
      subMap1.maps.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.right.value shouldBe List((3, "sub map 2"))
      subMap1.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous }.right.value shouldBe List((2, "two"), (1, "one"))
      subMap1.maps.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous }.right.value shouldBe List((3, "sub map 2"))
      subMap1.reverse.map(keyValue => keyValue).materialize.runRandomIO.right.value shouldBe List((2, "two"), (1, "one"))
      subMap1.maps.reverse.map(keyValue => keyValue).materialize.runRandomIO.right.value shouldBe List((3, "sub map 2"))
      subMap1.maps.reverse.map(keyValue => keyValue).materialize.runRandomIO.right.value shouldBe List((3, "sub map 2"))
      subMap1.reverse.take(100).materialize.runRandomIO.right.value shouldBe List((2, "two"), (1, "one"))
      subMap1.maps.reverse.take(100).materialize.runRandomIO.right.value shouldBe List((3, "sub map 2"))
      subMap1.maps.reverse.take(100).materialize.runRandomIO.right.value shouldBe List((3, "sub map 2"))
      subMap1.reverse.take(3).materialize.runRandomIO.right.value shouldBe List((2, "two"), (1, "one"))
      subMap1.maps.reverse.take(3).materialize.runRandomIO.right.value shouldBe List((3, "sub map 2"))
      subMap1.reverse.take(1).materialize.runRandomIO.right.value should contain only ((2, "two"))
      subMap1.maps.reverse.take(1).materialize.runRandomIO.right.value should contain only ((3, "sub map 2"))
      subMap1.take(100).materialize.runRandomIO.right.value should contain only((1, "one"), (2, "two"))
      subMap1.maps.take(100).materialize.runRandomIO.right.value should contain only ((3, "sub map 2"))
      subMap1.take(2).materialize.runRandomIO.right.value should contain only((1, "one"), (2, "two"))
      subMap1.maps.take(2).materialize.runRandomIO.right.value should contain only ((3, "sub map 2"))
      subMap1.take(1).materialize.runRandomIO.right.value should contain only ((1, "one"))
      subMap1.maps.take(1).materialize.runRandomIO.right.value should contain only ((3, "sub map 2"))
      subMap1.reverse.drop(1).materialize.runRandomIO.right.value should contain only ((1, "one"))
      subMap1.maps.reverse.drop(1).materialize.runRandomIO.right.value shouldBe empty
      subMap1.drop(1).materialize.runRandomIO.right.value should contain only ((2, "two"))
      subMap1.maps.drop(1).materialize.runRandomIO.right.value shouldBe empty
      subMap1.reverse.drop(0).materialize.runRandomIO.right.value shouldBe List((2, "two"), (1, "one"))
      subMap1.maps.reverse.drop(0).materialize.runRandomIO.right.value shouldBe List((3, "sub map 2"))
      subMap1.drop(0).materialize.runRandomIO.right.value shouldBe List((1, "one"), (2, "two"))
      subMap1.maps.drop(0).materialize.runRandomIO.right.value shouldBe List((3, "sub map 2"))

      //KEYS ONLY ITERATIONS
      subMap1.keys.size.right.value shouldBe 2
      subMap1.keys.headOption.right.value.value shouldBe 1
      subMap1.keys.lastOption.right.value.value shouldBe 2
      //      subMap1.maps.keys.lastOption.runIO shouldBe 3
      //      subMap1.maps.keys.toSeq.right.value shouldBe List(3)

      //SECOND MAP ITERATIONS
      subMap2.size.right.value shouldBe 2
      subMap2.headOption.right.value.value shouldBe ((3, "three"))
      subMap2.lastOption.right.value.value shouldBe ((4, "four"))
      subMap2.map(keyValue => (keyValue._1, keyValue._2)).materialize.runRandomIO.right.value shouldBe List((3, "three"), (4, "four"))
      subMap2.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue }.right.value shouldBe List((3, "three"), (4, "four"))
      subMap2.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous }.right.value shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.map(keyValue => keyValue).materialize.runRandomIO.right.value shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.take(100).materialize.runRandomIO.right.value shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.take(2).materialize.runRandomIO.right.value shouldBe List((4, "four"), (3, "three"))
      subMap2.reverse.take(1).materialize.runRandomIO.right.value should contain only ((4, "four"))
      subMap2.take(100).materialize.runRandomIO.right.value should contain only((3, "three"), (4, "four"))
      subMap2.take(2).materialize.runRandomIO.right.value should contain only((3, "three"), (4, "four"))
      subMap2.take(1).materialize.runRandomIO.right.value should contain only ((3, "three"))
      subMap2.reverse.drop(1).materialize.runRandomIO.right.value should contain only ((3, "three"))
      subMap2.drop(1).materialize.runRandomIO.right.value should contain only ((4, "four"))
      subMap2.reverse.drop(0).materialize.runRandomIO.right.value shouldBe List((4, "four"), (3, "three"))
      subMap2.drop(0).materialize.runRandomIO.right.value shouldBe List((3, "three"), (4, "four"))

      db.closeDatabase().right.value
    }
  }
}
