/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import scala.concurrent.duration._
import swaydb.data.util.StorageUnits._

class SwayDBSubMapPutSpec0 extends SwayDBSubMapPutSpec {
  val keyValueCount: Int = 1000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    SwayDB.persistent[Key[Int], Option[String]](dir = randomDir, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet.extend.assertGet
}

class SwayDBSubMapPutSpec1 extends SwayDBSubMapPutSpec {

  val keyValueCount: Int = 10000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    SwayDB.persistent[Key[Int], Option[String]](randomDir, mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet.extend.assertGet
}

class SwayDBSubMapPutSpec2 extends SwayDBSubMapPutSpec {

  val keyValueCount: Int = 100000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    SwayDB.memory[Key[Int], Option[String]](mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet.extend.assertGet
}

class SwayDBSubMapPutSpec3 extends SwayDBSubMapPutSpec {
  val keyValueCount: Int = 100000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    SwayDB.memory[Key[Int], Option[String]](minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet.extend.assertGet
}

sealed trait SwayDBSubMapPutSpec extends TestBase with TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds): Map[Int, String]

  "Root" should {
    "Initialise a RootMap & SubMap from Root" in {
      val db = newDB()

      val rootMap = db.putMap(1, "rootMap").assertGet
      val firstMap = rootMap.putMap(2, "first map").assertGet

      firstMap.put(3, "three").assertGet
      firstMap.put(4, "four").assertGet
      firstMap.put(5, "five").assertGet
      firstMap.put(4, "four again").assertGet

      firstMap.includeMaps().toList should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
    }

    "Initialise a RootMap & 2 SubMaps from Root" in {
      val db = newDB()

      def insert(firstMap: Map[Int, String]) = {
        firstMap.put(3, "three").assertGet
        firstMap.put(4, "four").assertGet
        firstMap.put(5, "five").assertGet
        firstMap.put(4, "four again").assertGet
      }

      val firstMap = db.putMap(1, "first map").assertGet

      val secondMap = firstMap.putMap(2, "second map").assertGet
      insert(secondMap)

      val thirdMap = firstMap.putMap(3, "third map").assertGet
      insert(thirdMap)

      secondMap.toList should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
      thirdMap.toList should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
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
    //        val firstMap = rootMap.putMap(2, "first map").assertGet
    //        insertInMap(firstMap)
    //
    //        val secondMap = rootMap.putMap(3, "second map").assertGet
    //        insertInMap(secondMap)
    //      }
    //
    //      insertInRoot(db)
    //
    //      rootMap1.putMap(2, "first map").assertGet.toList should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
    //      rootMap1.putMap(3, "second map").assertGet.toList should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
    //      rootMap2.putMap(2, "first map").assertGet.toList should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
    //      rootMap2.putMap(3, "second map").assertGet.toList should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
    //    }

    "Initialise 5 nested maps with 2 elements in each map" in {
      val db = newDB()

      val rootMap = db.putMap(1, "rootMap1").assertGet

      val subMap1 = rootMap.putMap(2, "sub map 2").assertGet
      subMap1.put(1, "one").assertGet
      subMap1.put(2, "two").assertGet

      val subMap2 = subMap1.putMap(3, "sub map three").assertGet
      subMap2.put(3, "three").assertGet
      subMap2.put(4, "four").assertGet

      val subMap3 = subMap2.putMap(5, "sub map five").assertGet
      subMap3.put(5, "five").assertGet
      subMap3.put(6, "six").assertGet

      val subMap4 = subMap3.putMap(7, "sub map seven").assertGet
      subMap4.put(7, "seven").assertGet
      subMap4.put(8, "eight").assertGet

      subMap1.includeMaps().toList should contain inOrderOnly((1, "one"), (2, "two"), (3, "sub map three"))
      subMap2.includeMaps().toList should contain inOrderOnly((3, "three"), (4, "four"), (5, "sub map five"))
      subMap3.includeMaps().toList should contain inOrderOnly((5, "five"), (6, "six"), (7, "sub map seven"))
      subMap4.includeMaps().toList should contain inOrderOnly((7, "seven"), (8, "eight"))
    }

    "Initialise 5 sibling maps with 2 elements in each map" in {
      val db = newDB()

      val rootMap = db.putMap(1, "rootMap1").assertGet

      val subMap1 = rootMap.putMap(2, "sub map 2").assertGet
      subMap1.put(1, "one").assertGet
      subMap1.put(2, "two").assertGet

      val subMap2 = rootMap.putMap(3, "sub map three").assertGet
      subMap2.put(3, "three").assertGet
      subMap2.put(4, "four").assertGet

      val subMap3 = rootMap.putMap(5, "sub map five").assertGet
      subMap3.put(5, "five").assertGet
      subMap3.put(6, "six").assertGet

      val subMap4 = rootMap.putMap(7, "sub map seven").assertGet
      subMap4.put(7, "seven").assertGet
      subMap4.put(8, "eight").assertGet

      subMap1.toList should contain inOrderOnly((1, "one"), (2, "two"))
      subMap2.toList should contain inOrderOnly((3, "three"), (4, "four"))
      subMap3.toList should contain inOrderOnly((5, "five"), (6, "six"))
      subMap4.toList should contain inOrderOnly((7, "seven"), (8, "eight"))
    }
  }
}