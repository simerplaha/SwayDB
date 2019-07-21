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
import swaydb.serializers.Default._
import org.scalatest.OptionValues._

class MapPutSpec0 extends MapPutSpec {
  val keyValueCount: Int = 1000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.persistent.Map[Int, String](dir = randomDir).runIO
}

class MapPutSpec1 extends MapPutSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.persistent.Map[Int, String](randomDir, mapSize = 1.byte).runIO
}

class MapPutSpec2 extends MapPutSpec {

  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.memory.Map[Int, String](mapSize = 1.byte).runIO
}

class MapPutSpec3 extends MapPutSpec {
  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.memory.Map[Int, String]().runIO
}

sealed trait MapPutSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): Map[Int, String]

  "Root" should {
    "Initialise a RootMap & SubMap from Root" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap").runIO
      val firstMap = rootMap.maps.put(2, "first map").runIO

      firstMap.put(3, "three").runIO
      firstMap.put(4, "four").runIO
      firstMap.put(5, "five").runIO
      firstMap.put(4, "four again").runIO

      firstMap
        .stream
        .materialize.runIO should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))

      db.closeDatabase().get
    }

    "Initialise a RootMap & 2 SubMaps from Root" in {
      val db = newDB()

      def insert(firstMap: Map[Int, String]) = {
        firstMap.put(3, "three").runIO
        firstMap.put(4, "four").runIO
        firstMap.put(5, "five").runIO
        firstMap.put(4, "four again").runIO
      }

      val firstMap = db.maps.put(1, "first map").runIO

      val secondMap = firstMap.maps.put(2, "second map").runIO
      insert(secondMap)

      val thirdMap = firstMap.maps.put(3, "third map").runIO
      insert(thirdMap)

      secondMap
        .stream
        .materialize.runIO should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))

      thirdMap
        .stream
        .materialize.runIO should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))

      db.closeDatabase().get
    }

    //    "Initialise 2 RootMaps & 2 SubMaps under each SubMap" in {
    //      val db = newDB()
    //
    //      def insertInMap(firstMap) = {
    //        firstMap.put(3, "three").assertGet
    //        firstMap.put(4, "four").assertGet
    //        firstMap.put(5, "five").assertGet
    //        firstMap.put(4, "four again").assertGet
    //      }
    //
    //      def insertInRoot(rootMap: Root[Int, String]) = {
    //        val firstMap = rootMap.maps.put(2, "first map").assertGet
    //        insertInMap(firstMap)
    //
    //        val secondMap = rootMap.maps.put(3, "second map").assertGet
    //        insertInMap(secondMap)
    //      }
    //
    //      insertInRoot(db)
    //
    //      rootMap1.maps.put(2, "first map").assertGet.toSeq.get should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
    //      rootMap1.maps.put(3, "second map").assertGet.toSeq.get should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
    //      rootMap2.maps.put(2, "first map").assertGet.toSeq.get should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
    //      rootMap2.maps.put(3, "second map").assertGet.toSeq.get should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
    //    }

    "putOrGet spec" in {
      val db = newDB()

      val map = db.maps.getOrPut(1, "firstMap").runIO
      map.exists().runIO shouldBe true
      map.getValue().runIO.value shouldBe "firstMap"

      val mapAgain = db.maps.getOrPut(1, "firstMap put again").runIO
      mapAgain.exists().runIO shouldBe true
      //value does not change as the map already exists.
      mapAgain.getValue().runIO.value shouldBe "firstMap"

      db.closeDatabase().get
    }

    "Initialise 5 nested maps with 2 elements in each map" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap1").runIO

      val subMap1 = rootMap.maps.put(2, "sub map 2").runIO
      subMap1.put(1, "one").runIO
      subMap1.put(2, "two").runIO

      val subMap2 = subMap1.maps.put(3, "sub map three").runIO
      subMap2.put(3, "three").runIO
      subMap2.put(4, "four").runIO

      val subMap3 = subMap2.maps.put(5, "sub map five").runIO
      subMap3.put(5, "five").runIO
      subMap3.put(6, "six").runIO

      val subMap4 = subMap3.maps.put(7, "sub map seven").runIO
      subMap4.put(7, "seven").runIO
      subMap4.put(8, "eight").runIO

      subMap1.stream.materialize.runIO should contain inOrderOnly((1, "one"), (2, "two"))
      subMap1.maps.stream.materialize.runIO should contain only ((3, "sub map three"))
      subMap2.stream.materialize.runIO should contain inOrderOnly((3, "three"), (4, "four"))
      subMap2.maps.stream.materialize.runIO should contain only ((5, "sub map five"))
      subMap3.stream.materialize.runIO should contain inOrderOnly((5, "five"), (6, "six"))
      subMap3.maps.stream.materialize.runIO should contain only ((7, "sub map seven"))
      subMap4.stream.materialize.runIO should contain inOrderOnly((7, "seven"), (8, "eight"))

      db.closeDatabase().get
    }

    "Initialise 5 sibling maps with 2 elements in each map" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap1").runIO

      val subMap1 = rootMap.maps.put(2, "sub map 2").runIO
      subMap1.put(1, "one").runIO
      subMap1.put(2, "two").runIO

      val subMap2 = rootMap.maps.put(3, "sub map three").runIO
      subMap2.put(3, "three").runIO
      subMap2.put(4, "four").runIO

      val subMap3 = rootMap.maps.put(5, "sub map five").runIO
      subMap3.put(5, "five").runIO
      subMap3.put(6, "six").runIO

      val subMap4 = rootMap.maps.put(7, "sub map seven").runIO
      subMap4.put(7, "seven").runIO
      subMap4.put(8, "eight").runIO

      subMap1.stream.materialize.runIO should contain inOrderOnly((1, "one"), (2, "two"))
      subMap2.stream.materialize.runIO should contain inOrderOnly((3, "three"), (4, "four"))
      subMap3.stream.materialize.runIO should contain inOrderOnly((5, "five"), (6, "six"))
      subMap4.stream.materialize.runIO should contain inOrderOnly((7, "seven"), (8, "eight"))

      db.closeDatabase().get
    }
  }
}
