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

package swaydb.multimap

import swaydb.api.TestBaseEmbedded
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.{Bag, MultiMap}
import swaydb.core.RunThis._
import swaydb.core.CommonAssertions._

import scala.util.Random

class FromMapSpec0 extends FromMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): MultiMap[Int, String, Nothing, Bag.Less] =
    swaydb.persistent.MultiMap[Int, String, Nothing, Bag.Less](dir = randomDir)
}

class FromMapSpec1 extends FromMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): MultiMap[Int, String, Nothing, Bag.Less] =
    swaydb.persistent.MultiMap[Int, String, Nothing, Bag.Less](dir = randomDir, mapSize = 1.byte)
}

class FromMapSpec2 extends FromMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): MultiMap[Int, String, Nothing, Bag.Less] =
    swaydb.memory.MultiMap[Int, String, Nothing, Bag.Less]()
}

class FromMapSpec3 extends FromMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): MultiMap[Int, String, Nothing, Bag.Less] =
    swaydb.memory.MultiMap[Int, String, Nothing, Bag.Less](mapSize = 1.byte)
}

sealed trait FromMapSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): MultiMap[Int, String, Nothing, Bag.Less]

  implicit val bag = Bag.less

  "From" should {

    "return empty on an empty Map" in {
      val root = newDB()

      val rootMap = root.children.init(1)

      rootMap.stream.materialize.toList shouldBe empty
      rootMap.from(1).stream.materialize.toList shouldBe empty
      rootMap.children.keys.materialize.toList shouldBe empty

      root.delete()
    }

    "if the map contains multiple non empty subMap" in {
      runThis(100.times, log = true) {
        val root = newDB()

        //map hierarchy
        //rootMap
        //   |_____ (1, "one")
        //          (2, "two")
        //          (3, "three")
        //          (4, "four")
        //              maps ---> (2, "sub map")
        //                |              |___________ (11, "one one")
        //                |                           (22, "two two")
        //                |                           (33, "three three")
        //                |                           (44, "four four")
        //                |-----> (3, "sub map")
        //                              |___________ (111, "one one one")
        //                                           (222, "two two two")
        //                                           (333, "three three three")
        //                                           (444, "four four four")
        val rootMap = root.children.init(1)
        val childMap1 = rootMap.children.init(2)
        val childMap2 = rootMap.children.init(3)

        def doInserts(skipRandomly: Boolean) = {
          //insert entries to rootMap
          def skip = skipRandomly && Random.nextBoolean()

          if (!skip)
            Seq(
              () => rootMap.put(1, "one"),
              () => rootMap.put(2, "two"),
              () => rootMap.put(3, "three"),
              () => rootMap.put(4, "four")
            ).runThisRandomly

          if (!skip)
            Seq(
              () => childMap1.put(11, "one one"),
              () => childMap1.put(22, "two two"),
              () => childMap1.put(33, "three three"),
              () => childMap1.put(44, "four four")
            ).runThisRandomly

          if (!skip)
            Seq(
              () => childMap2.put(111, "one one one"),
              () => childMap2.put(222, "two two two"),
              () => childMap2.put(333, "three three three"),
              () => childMap2.put(444, "four four four")
            ).runThisRandomly
        }

        //perform initial write.
        doInserts(skipRandomly = false)

        //random read in random order
        runThisParallel(100.times) {
          Seq(
            //randomly also perform insert which would just create duplicate data.
            () => eitherOne(doInserts(skipRandomly = true), (), ()),

            () => rootMap.children.keys.materialize.toList should contain only(2, 3),
            () => rootMap.from(1).stream.materialize.toList shouldBe List((1, "one"), (2, "two"), (3, "three"), (4, "four")),
            //reverse from the map.
            () => rootMap.before(2).reverse.stream.map { case (key, value) => (key, value) }.materialize shouldBe List((1, "one")),
            () => rootMap.before(3).reverse.stream.map { case (key, value) => (key, value) }.materialize shouldBe List((2, "two"), (1, "one")),
            () => rootMap.before(4).reverse.stream.map { case (key, value) => (key, value) }.materialize shouldBe List((3, "three"), (2, "two"), (1, "one")),
            () => rootMap.before(5).reverse.stream.map { case (key, value) => (key, value) }.materialize shouldBe List((4, "four"), (3, "three"), (2, "two"), (1, "one")),
            () => rootMap.from(3).reverse.stream.map { case (key, value) => (key, value) }.materialize shouldBe List((3, "three"), (2, "two"), (1, "one")),

            //forward from entry
            () => rootMap.from(1).stream.materialize shouldBe List((1, "one"), (2, "two"), (3, "three"), (4, "four")),
            () => rootMap.fromOrAfter(1).stream.materialize shouldBe List((1, "one"), (2, "two"), (3, "three"), (4, "four")),
            () => rootMap.fromOrBefore(1).stream.materialize shouldBe List((1, "one"), (2, "two"), (3, "three"), (4, "four")),
            () => rootMap.after(2).stream.materialize shouldBe List((3, "three"), (4, "four")),

            () => childMap1.stream.materialize shouldBe List((11, "one one"), (22, "two two"), (33, "three three"), (44, "four four")),
            () => childMap1.from(11).stream.materialize shouldBe List((11, "one one"), (22, "two two"), (33, "three three"), (44, "four four")),
            () => childMap1.from(22).stream.materialize shouldBe List((22, "two two"), (33, "three three"), (44, "four four")),
            () => childMap1.from(33).stream.materialize shouldBe List((33, "three three"), (44, "four four")),
            () => childMap1.from(44).stream.materialize shouldBe List((44, "four four")),

            () => childMap1.from(11).reverse.stream.map { case (key, value) => (key, value) }.materialize shouldBe List((11, "one one")),
            () => childMap1.from(22).reverse.stream.map { case (key, value) => (key, value) }.materialize shouldBe List((22, "two two"), (11, "one one")),
            () => childMap1.from(33).reverse.stream.map { case (key, value) => (key, value) }.materialize shouldBe List((33, "three three"), (22, "two two"), (11, "one one")),
            () => childMap1.from(44).reverse.stream.map { case (key, value) => (key, value) }.materialize shouldBe List((44, "four four"), (33, "three three"), (22, "two two"), (11, "one one")),

            () => childMap1.children.stream.materialize shouldBe empty
          ).runThisRandomly
        }

        root.delete()
      }
    }
  }
}
