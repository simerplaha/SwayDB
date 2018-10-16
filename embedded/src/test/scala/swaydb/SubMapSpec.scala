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
import swaydb.serializers.Default._

import scala.concurrent.duration._

class SubMapSpec0 extends SubMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Root[Int, String] =
    SwayDB.enableExtensions.persistent[Int, String](dir = randomDir, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

class SubMapSpec1 extends SubMapSpec {

  val keyValueCount: Int = 10000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Root[Int, String] =
    SwayDB.enableExtensions.persistent[Int, String](randomDir, mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

class SubMapSpec2 extends SubMapSpec {

  val keyValueCount: Int = 100000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Root[Int, String] =
    SwayDB.enableExtensions.memory[Int, String](mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

class SubMapSpec3 extends SubMapSpec {
  val keyValueCount: Int = 100000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Root[Int, String] =
    SwayDB.enableExtensions.memory[Int, String](minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

sealed trait SubMapSpec extends TestBase with TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds): Root[Int, String]

  implicit val mapKeySerializer = MapKey.mapKeySerializer(IntSerializer)

  "SubMap" should {

    "return entries ranges" in {
      SubMap.entriesRangeKeys(Seq(1, 2, 3)) shouldBe ((MapKey.EntriesStart(Seq(1, 2, 3)), MapKey.EntriesEnd(Seq(1, 2, 3))))
    }

    "return empty subMap range keys for a empty SubMap" in {
      val db = newDB()

      val rootMap = db.createMap(1, "rootMap").assertGet
      SubMap.childSubMapRanges(rootMap) shouldBe empty

    }

    "return 1 subMap range keys for a SubMap with 1 child SubMap" in {
      val db = newDB()

      val firstMap = db.createMap(1, "rootMap").assertGet
      val secondMap = firstMap.putMap(2, "second map").assertGet
      val thirdMap = secondMap.putMap(3, "third map").assertGet

      SubMap.childSubMapRanges(firstMap) should contain inOrderOnly((MapKey.Start(Seq(1, 2)), MapKey.End(Seq(1, 2))), (MapKey.Start(Seq(1, 2, 3)), MapKey.End(Seq(1, 2, 3))))
      SubMap.childSubMapRanges(secondMap) should contain only ((MapKey.Start(Seq(1, 2, 3)), MapKey.End(Seq(1, 2, 3))))
      SubMap.childSubMapRanges(thirdMap) shouldBe empty
    }

    "returns sub maps when child subMap has the same id as parent" in {
      val db = newDB()

      val firstMap = db.createMap(1, "first").assertGet
      val secondMap = firstMap.putMap(2, "second").assertGet
      val thirdMap = secondMap.putMap(2, "third").assertGet

      SubMap.childSubMapRanges(firstMap) should contain inOrderOnly((MapKey.Start(Seq(1, 2)), MapKey.End(Seq(1, 2))), (MapKey.Start(Seq(1, 2, 2)), MapKey.End(Seq(1, 2, 2))))
      SubMap.childSubMapRanges(secondMap) should contain only ((MapKey.Start(Seq(1, 2, 2)), MapKey.End(Seq(1, 2, 2))))
      SubMap.childSubMapRanges(thirdMap) shouldBe empty
    }

    "returns multiple child subMap that also contains nested subMaps" in {
      val db = newDB()

      val firstMap = db.createMap(1, "firstMap").assertGet
      val secondMap: SubMap[Int, String] = firstMap.putMap(2, "subMap").assertGet

      secondMap.putMap(2, "subMap").assertGet
      secondMap.putMap(3, "subMap3").assertGet
      val subMap4 = secondMap.putMap(4, "subMap4").assertGet
      subMap4.putMap(44, "subMap44").assertGet
      val subMap5 = secondMap.putMap(5, "subMap5").assertGet
      val subMap55 = subMap5.putMap(55, "subMap55").assertGet
      subMap55.putMap(5555, "subMap55").assertGet
      subMap55.putMap(6666, "subMap55").assertGet
      subMap5.putMap(555, "subMap555").assertGet

      SubMap.childSubMapRanges(firstMap) shouldBe
        List(
          (MapKey.Start(Seq(1, 2)), MapKey.End(Seq(1, 2))),
          (MapKey.Start(Seq(1, 2, 2)), MapKey.End(Seq(1, 2, 2))),
          (MapKey.Start(Seq(1, 2, 3)), MapKey.End(Seq(1, 2, 3))),
          (MapKey.Start(Seq(1, 2, 4)), MapKey.End(Seq(1, 2, 4))),
          (MapKey.Start(Seq(1, 2, 4, 44)), MapKey.End(Seq(1, 2, 4, 44))),
          (MapKey.Start(Seq(1, 2, 5)), MapKey.End(Seq(1, 2, 5))),
          (MapKey.Start(Seq(1, 2, 5, 55)), MapKey.End(Seq(1, 2, 5, 55))),
          (MapKey.Start(Seq(1, 2, 5, 55, 5555)), MapKey.End(Seq(1, 2, 5, 55, 5555))),
          (MapKey.Start(Seq(1, 2, 5, 55, 6666)), MapKey.End(Seq(1, 2, 5, 55, 6666))),
          (MapKey.Start(Seq(1, 2, 5, 555)), MapKey.End(Seq(1, 2, 5, 555)))
        )

      SubMap.childSubMapRanges(secondMap) shouldBe
        List(
          (MapKey.Start(Seq(1, 2, 2)), MapKey.End(Seq(1, 2, 2))),
          (MapKey.Start(Seq(1, 2, 3)), MapKey.End(Seq(1, 2, 3))),
          (MapKey.Start(Seq(1, 2, 4)), MapKey.End(Seq(1, 2, 4))),
          (MapKey.Start(Seq(1, 2, 4, 44)), MapKey.End(Seq(1, 2, 4, 44))),
          (MapKey.Start(Seq(1, 2, 5)), MapKey.End(Seq(1, 2, 5))),
          (MapKey.Start(Seq(1, 2, 5, 55)), MapKey.End(Seq(1, 2, 5, 55))),
          (MapKey.Start(Seq(1, 2, 5, 55, 5555)), MapKey.End(Seq(1, 2, 5, 55, 5555))),
          (MapKey.Start(Seq(1, 2, 5, 55, 6666)), MapKey.End(Seq(1, 2, 5, 55, 6666))),
          (MapKey.Start(Seq(1, 2, 5, 555)), MapKey.End(Seq(1, 2, 5, 555)))
        )
    }
  }

  "SubMap" when {
    "putMap" should {
      "create a subMap" in {
        val root = newDB()

        val first = root.createMap(1, "first").assertGet
        val second = first.putMap(2, "second").assertGet
        first.getMap(2).assertGetOpt shouldBe defined
        second.getMap(2).assertGetOpt shouldBe empty

        root.innerMap() foreach println
      }
    }
  }
}