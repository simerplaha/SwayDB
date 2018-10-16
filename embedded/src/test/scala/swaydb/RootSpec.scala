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

package swaydb

import swaydb.core.TestBase
import swaydb.data.map.MapKey
import swaydb.extension.Root
import swaydb.serializers.Default._

import scala.concurrent.duration._

class RootSpec0 extends RootSpec {
  val keyValueCount: Int = 1000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Root[Int, String] =
    SwayDB.enableExtensions.persistent[Int, String](dir = randomDir, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

class RootSpec1 extends RootSpec {

  val keyValueCount: Int = 10000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Root[Int, String] =
    SwayDB.enableExtensions.persistent[Int, String](randomDir, mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

class RootSpec2 extends RootSpec {

  val keyValueCount: Int = 100000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Root[Int, String] =
    SwayDB.enableExtensions.memory[Int, String](mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

class RootSpec3 extends RootSpec {
  val keyValueCount: Int = 100000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Root[Int, String] =
    SwayDB.enableExtensions.memory[Int, String](minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

sealed trait RootSpec extends TestBase with TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds): Root[Int, String]

  implicit val mapKeySerializer = MapKey.mapKeySerializer(IntSerializer)

  "Root on single subMap" should {
    "create a map" in {
      val emptyMap = newDB()

      emptyMap.createMap(1, "rootMap").assertGet

      emptyMap.mapExists(1).assertGet shouldBe true
      emptyMap.getMap(1).assertGetOpt shouldBe defined
      emptyMap.getMapValue(1).assertGet shouldBe "rootMap"
    }

    "remove a map" in {
      val emptyMap = newDB()

      emptyMap.createMap(1, "rootMap").assertGet
      emptyMap.mapExists(1).assertGet shouldBe true

      emptyMap.removeMap(1).assertGet
      emptyMap.mapExists(1).assertGet shouldBe false
    }

    "get a maps value" in {
      val emptyMap = newDB()

      emptyMap.createMap(1, "rootMap").assertGet
      emptyMap.getMapValue(1).assertGet shouldBe "rootMap"
    }
  }

  "Root on multiple subMap" should {
    "create & remove maps" in {
      val emptyMap = newDB()

      //create subMaps
      (1 to 100) foreach {
        i =>
          emptyMap.createMap(i, s"subMap$i").assertGet

          (1 to i) foreach {
            i =>
              emptyMap.mapExists(i).assertGet shouldBe true
              emptyMap.getMap(i).assertGetOpt shouldBe defined
              emptyMap.getMapValue(i).assertGet shouldBe s"subMap$i"
          }
      }

      //database is not empty
      emptyMap.innerMap().toList should not be empty

      //remove all maps
      (1 to 100) foreach {
        i =>
          emptyMap.removeMap(i).assertGet

          (1 to i) foreach {
            i =>
              emptyMap.mapExists(i).assertGet shouldBe false
              emptyMap.getMap(i).assertGetOpt shouldBe empty
              emptyMap.getMapValue(i).assertGetOpt shouldBe empty
          }
      }

      //database is empty
      emptyMap.innerMap().toList shouldBe empty
    }
  }
}