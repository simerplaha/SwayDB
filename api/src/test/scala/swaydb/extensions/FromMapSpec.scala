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
import swaydb.core.IOValues._
import swaydb.core.RunThis._
import swaydb.data.util.StorageUnits._
import swaydb.extensions
import swaydb.serializers.Default._
import org.scalatest.OptionValues._

class FromMapSpec0 extends FromMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): extensions.Map[Int, String] =
    swaydb.extensions.persistent.Map[Int, String](dir = randomDir).runIO
}

class FromMapSpec1 extends FromMapSpec {

  val keyValueCount: Int = 10000

  override def newDB(): extensions.Map[Int, String] =
    swaydb.extensions.persistent.Map[Int, String](randomDir, mapSize = 1.byte).runIO
}

class FromMapSpec2 extends FromMapSpec {

  val keyValueCount: Int = 100000

  override def newDB(): extensions.Map[Int, String] =
    swaydb.extensions.memory.Map[Int, String](mapSize = 1.byte).runIO
}

class FromMapSpec3 extends FromMapSpec {
  val keyValueCount: Int = 100000

  override def newDB(): extensions.Map[Int, String] =
    swaydb.extensions.memory.Map[Int, String]().runIO
}

sealed trait FromMapSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): extensions.Map[Int, String]

  "From" should {

    "return empty on an empty Map" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap").runIO

      rootMap.maps.from(1).stream.materialize.runIO shouldBe empty
      rootMap.maps.before(1).stream.materialize.runIO shouldBe empty
      rootMap.maps.after(1).stream.materialize.runIO shouldBe empty
      rootMap.fromOrBefore(1).stream.materialize.runIO shouldBe empty
      rootMap.fromOrAfter(1).stream.materialize.runIO shouldBe empty

      db.closeDatabase().get
    }

    "if the map contains only 1 empty subMap" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap").runIO
      val firstMap = rootMap.maps.put(2, "sub map").runIO

      rootMap.maps.from(1).stream.materialize.runIO shouldBe empty
      rootMap.maps.before(1).stream.materialize.runIO shouldBe List((2, "sub map"))
      rootMap.maps.after(1).stream.materialize.runIO shouldBe List((2, "sub map"))
      rootMap.maps.fromOrBefore(1).stream.materialize.runIO shouldBe List((2, "sub map"))
      rootMap.maps.fromOrAfter(1).stream.materialize.runIO shouldBe List((2, "sub map"))

      rootMap.maps.from(2).stream.materialize.runIO shouldBe List((2, "sub map"))
      rootMap.maps.before(2).stream.materialize.runIO shouldBe List((2, "sub map"))
      rootMap.maps.after(2).stream.materialize.runIO shouldBe empty
      rootMap.maps.fromOrBefore(2).stream.materialize.runIO shouldBe List((2, "sub map"))
      rootMap.maps.fromOrAfter(2).stream.materialize.runIO shouldBe List((2, "sub map"))

      rootMap.maps.stream.materialize.runIO should have size 1
      firstMap.stream.materialize.runIO shouldBe empty
      firstMap.maps.stream.materialize.runIO shouldBe empty

      rootMap.maps.headOption.get should contain((2, "sub map"))
      rootMap.maps.lastOption.get should contain((2, "sub map"))

      db.closeDatabase().get
    }

    "if the map contains multiple non empty subMap" in {
      val db = newDB()

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
      val rootMap = db.maps.put(1, "rootMap").runIO
      val firstMap = rootMap.maps.put(2, "sub map").runIO
      val secondMap = rootMap.maps.put(3, "sub map").runIO

      //insert entries to rootMap
      rootMap.put(1, "one").runIO
      rootMap.put(2, "two").runIO
      rootMap.put(3, "three").runIO
      rootMap.put(4, "four").runIO

      //insert entries to firstMap
      firstMap.put(11, "one one").runIO
      firstMap.put(22, "two two").runIO
      firstMap.put(33, "three three").runIO
      firstMap.put(44, "four four").runIO

      //insert entries to firstMap
      secondMap.put(111, "one one one").runIO
      secondMap.put(222, "two two two").runIO
      secondMap.put(333, "three three three").runIO
      secondMap.put(444, "four four four").runIO

      rootMap.maps.from(1).stream.materialize.runIO shouldBe empty
      //reverse from the map.
      rootMap.before(2).reverse.map { case (key, value) => (key, value) }.materialize.runIO shouldBe List((1, "one"))
      rootMap.before(3).reverse.map { case (key, value) => (key, value) }.materialize.runIO shouldBe List((2, "two"), (1, "one"))
      rootMap.before(4).reverse.map { case (key, value) => (key, value) }.materialize.runIO shouldBe List((3, "three"), (2, "two"), (1, "one"))
      rootMap.before(5).reverse.map { case (key, value) => (key, value) }.materialize.runIO shouldBe List((4, "four"), (3, "three"), (2, "two"), (1, "one"))
      rootMap.from(3).reverse.map { case (key, value) => (key, value) }.materialize.runIO shouldBe List((3, "three"), (2, "two"), (1, "one"))

      rootMap.maps.before(3).reverse.map { case (key, value) => (key, value) }.materialize.runIO shouldBe List((2, "sub map"))
      rootMap.maps.from(3).reverse.map { case (key, value) => (key, value) }.materialize.runIO shouldBe List((3, "sub map"), (2, "sub map"))

      //forward from entry
      rootMap.from(1).stream.materialize.runIO shouldBe List((1, "one"), (2, "two"), (3, "three"), (4, "four"))
      rootMap.fromOrAfter(1).stream.materialize.runIO shouldBe List((1, "one"), (2, "two"), (3, "three"), (4, "four"))
      rootMap.fromOrBefore(1).stream.materialize.runIO shouldBe List((1, "one"), (2, "two"), (3, "three"), (4, "four"))
      rootMap.after(2).stream.materialize.runIO shouldBe List((3, "three"), (4, "four"))

      firstMap.stream.materialize.runIO shouldBe List((11, "one one"), (22, "two two"), (33, "three three"), (44, "four four"))
      firstMap.from(11).stream.materialize.runIO shouldBe List((11, "one one"), (22, "two two"), (33, "three three"), (44, "four four"))
      firstMap.from(22).stream.materialize.runIO shouldBe List((22, "two two"), (33, "three three"), (44, "four four"))
      firstMap.from(33).stream.materialize.runIO shouldBe List((33, "three three"), (44, "four four"))
      firstMap.from(44).stream.materialize.runIO shouldBe List((44, "four four"))

      firstMap.from(11).reverse.map { case (key, value) => (key, value) }.materialize.runIO shouldBe List((11, "one one"))
      firstMap.from(22).reverse.map { case (key, value) => (key, value) }.materialize.runIO shouldBe List((22, "two two"), (11, "one one"))
      firstMap.from(33).reverse.map { case (key, value) => (key, value) }.materialize.runIO shouldBe List((33, "three three"), (22, "two two"), (11, "one one"))
      firstMap.from(44).reverse.map { case (key, value) => (key, value) }.materialize.runIO shouldBe List((44, "four four"), (33, "three three"), (22, "two two"), (11, "one one"))

      firstMap.maps.stream.materialize.runIO shouldBe empty

      db.closeDatabase().get
    }
  }
}
