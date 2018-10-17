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
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.{SwayDB, TestBaseEmbedded}

import scala.concurrent.duration._

class MapSpec0 extends MapSpec {
  val keyValueCount: Int = 1000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    SwayDB.persistent[Key[Int], Option[String]](dir = randomDir, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet.extend.assertGet
}

class MapSpec1 extends MapSpec {

  val keyValueCount: Int = 10000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    SwayDB.persistent[Key[Int], Option[String]](randomDir, mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet.extend.assertGet
}

class MapSpec2 extends MapSpec {

  val keyValueCount: Int = 100000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    SwayDB.memory[Key[Int], Option[String]](mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet.extend.assertGet
}

class MapSpec3 extends MapSpec {
  val keyValueCount: Int = 100000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    SwayDB.memory[Key[Int], Option[String]](minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet.extend.assertGet
}

sealed trait MapSpec extends TestBase with TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds): Map[Int, String]

  implicit val mapKeySerializer = Key.serializer(IntSerializer)

  "SubMap on single subMap" should {
    "create a map" in {
      val rootMap = newDB()

      val firstMap = rootMap.putMap(1, "rootMap").assertGet

      rootMap.containsMap(1).assertGet shouldBe true
      rootMap.getMap(1).assertGetOpt shouldBe defined
      firstMap.getValue().assertGet shouldBe "rootMap"

      rootMap.innerMap() foreach println
    }

    "remove a map" in {
      val rootMap = newDB()

      val firstMap = rootMap.putMap(1, "rootMap").assertGet

      rootMap.containsMap(1).assertGet shouldBe true
      rootMap.removeMap(1).assertGet
      rootMap.containsMap(1).assertGet shouldBe false
      firstMap.exists().assertGet shouldBe false
    }

    "get a maps value" in {
      val rootMap = newDB()

      val firstMap = rootMap.putMap(1, "rootMap").assertGet
      firstMap.getValue().assertGet shouldBe "rootMap"
      rootMap.getValue().assertGetOpt shouldBe empty
    }
  }

  //  "SubMap on multiple subMap" should {
  //    "create & remove maps" in {
  //      val emptyMap = newDB()
  //
  //      //create subMaps
  //      (1 to 100) foreach {
  //        i =>
  //          val subMap = emptyMap.createMap(i, s"subMap$i").assertGet
  //          subMap.getValue().assertGet shouldBe s"subMap$i"
  //
  //          (1 to i) foreach {
  //            i =>
  //              emptyMap.containsMap(i).assertGet shouldBe true
  //              val subMap = emptyMap.getMap(i).assertGet
  //              subMap.getValue().assertGet shouldBe s"subMap$i"
  //          }
  //      }
  //
  //      //database is not empty
  //      emptyMap.innerMap().toList should not be empty
  //
  //      //remove all maps
  //      (1 to 100) foreach {
  //        i =>
  //          val subMap = emptyMap.getMap(i).assertGet
  //          subMap.remove().assertGet
  //          subMap.getValue().assertGetOpt shouldBe empty
  //          subMap.exists().assertGet shouldBe false
  //
  //          (1 to i) foreach {
  //            i =>
  //              emptyMap.containsMap(i).assertGet shouldBe false
  //              emptyMap.getMap(i).assertGetOpt shouldBe empty
  //          }
  //      }
  //
  //      //database is empty
  //      emptyMap.innerMap().toList shouldBe empty
  //    }
  //  }
}