///*
// * Copyright (C) 2018 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.extension
//
//import swaydb.{SwayDB, TestBaseEmbedded}
//import swaydb.core.TestBase
//import swaydb.extensions.MapKey
//import swaydb.serializers.Default._
//import swaydb.data.util.StorageUnits._
//
//import scala.concurrent.duration._
//
//class SubMapSpec0 extends SubMapSpec {
//  val keyValueCount: Int = 1000
//
//  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Root[Int, String] =
//    SwayDB.enableExtensions.persistent[Int, String](dir = randomDir, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
//}
//
//class SubMapSpec1 extends SubMapSpec {
//
//  val keyValueCount: Int = 10000
//
//  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Root[Int, String] =
//    SwayDB.enableExtensions.persistent[Int, String](randomDir, mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
//}
//
//class SubMapSpec2 extends SubMapSpec {
//
//  val keyValueCount: Int = 100000
//
//  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Root[Int, String] =
//    SwayDB.enableExtensions.memory[Int, String](mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
//}
//
//class SubMapSpec3 extends SubMapSpec {
//  val keyValueCount: Int = 100000
//
//  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Root[Int, String] =
//    SwayDB.enableExtensions.memory[Int, String](minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
//}
//
//sealed trait SubMapSpec extends TestBase with TestBaseEmbedded {
//
//  val keyValueCount: Int
//
//  def newDB(minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds): Root[Int, String]
//
//  implicit val mapKeySerializer = MapKey.mapKeySerializer(IntSerializer)
//
//  "SubMap" should {
//
//    "return entries ranges" in {
//      SubMap.entriesRangeKeys(Seq(1, 2, 3)) shouldBe ((MapKey.EntriesStart(Seq(1, 2, 3)), MapKey.EntriesEnd(Seq(1, 2, 3))))
//    }
//
//    "return empty subMap range keys for a empty SubMap" in {
//      val db = newDB()
//
//      val rootMap = db.createMap(1, "rootMap").assertGet
//      SubMap.childSubMapRanges(rootMap) shouldBe empty
//
//    }
//
//    "return subMap that has only one child subMap" in {
//      val db = newDB()
//
//      val firstMap = db.createMap(1, "rootMap").assertGet
//      val secondMap = firstMap.putMap(2, "second map").assertGet
//
//      SubMap.childSubMapRanges(firstMap) should contain only ((MapKey.SubMap(Seq(1), 2), MapKey.Start(Seq(1, 2)), MapKey.End(Seq(1, 2))))
//      SubMap.childSubMapRanges(secondMap) shouldBe empty
//    }
//
//    "return subMaps of 3 nested maps" in {
//      val db = newDB()
//
//      val firstMap = db.createMap(1, "first").assertGet
//      val secondMap = firstMap.putMap(2, "second").assertGet
//      val thirdMap = secondMap.putMap(2, "third").assertGet
//
//      SubMap.childSubMapRanges(firstMap) should contain inOrderOnly((MapKey.SubMap(Seq(1), 2), MapKey.Start(Seq(1, 2)), MapKey.End(Seq(1, 2))), (MapKey.SubMap(Seq(1, 2), 2), MapKey.Start(Seq(1, 2, 2)), MapKey.End(Seq(1, 2, 2))))
//      SubMap.childSubMapRanges(secondMap) should contain only ((MapKey.SubMap(Seq(1, 2), 2), MapKey.Start(Seq(1, 2, 2)), MapKey.End(Seq(1, 2, 2))))
//      SubMap.childSubMapRanges(thirdMap) shouldBe empty
//    }
//
//    "returns multiple child subMap that also contains nested subMaps" in {
//      val db = newDB()
//
//      val firstMap = db.createMap(1, "firstMap").assertGet
//      val secondMap: SubMap[Int, String] = firstMap.putMap(2, "subMap").assertGet
//
//      secondMap.putMap(2, "subMap").assertGet
//      secondMap.putMap(3, "subMap3").assertGet
//      val subMap4 = secondMap.putMap(4, "subMap4").assertGet
//      subMap4.putMap(44, "subMap44").assertGet
//      val subMap5 = secondMap.putMap(5, "subMap5").assertGet
//      val subMap55 = subMap5.putMap(55, "subMap55").assertGet
//      subMap55.putMap(5555, "subMap55").assertGet
//      subMap55.putMap(6666, "subMap55").assertGet
//      subMap5.putMap(555, "subMap555").assertGet
//
//      val mapHierarchy =
//        List(
//          (MapKey.SubMap(Seq(1), 2), MapKey.Start(Seq(1, 2)), MapKey.End(Seq(1, 2))),
//          (MapKey.SubMap(Seq(1, 2), 2), MapKey.Start(Seq(1, 2, 2)), MapKey.End(Seq(1, 2, 2))),
//          (MapKey.SubMap(Seq(1, 2), 3), MapKey.Start(Seq(1, 2, 3)), MapKey.End(Seq(1, 2, 3))),
//          (MapKey.SubMap(Seq(1, 2), 4), MapKey.Start(Seq(1, 2, 4)), MapKey.End(Seq(1, 2, 4))),
//          (MapKey.SubMap(Seq(1, 2, 4), 44), MapKey.Start(Seq(1, 2, 4, 44)), MapKey.End(Seq(1, 2, 4, 44))),
//          (MapKey.SubMap(Seq(1, 2), 5), MapKey.Start(Seq(1, 2, 5)), MapKey.End(Seq(1, 2, 5))),
//          (MapKey.SubMap(Seq(1, 2, 5), 55), MapKey.Start(Seq(1, 2, 5, 55)), MapKey.End(Seq(1, 2, 5, 55))),
//          (MapKey.SubMap(Seq(1, 2, 5, 55), 5555), MapKey.Start(Seq(1, 2, 5, 55, 5555)), MapKey.End(Seq(1, 2, 5, 55, 5555))),
//          (MapKey.SubMap(Seq(1, 2, 5, 55), 6666), MapKey.Start(Seq(1, 2, 5, 55, 6666)), MapKey.End(Seq(1, 2, 5, 55, 6666))),
//          (MapKey.SubMap(Seq(1, 2, 5), 555), MapKey.Start(Seq(1, 2, 5, 555)), MapKey.End(Seq(1, 2, 5, 555)))
//        )
//
//      SubMap.childSubMapRanges(firstMap) shouldBe mapHierarchy
//      SubMap.childSubMapRanges(secondMap) shouldBe mapHierarchy.drop(1)
//    }
//  }
//
//  "SubMap" when {
//    "putMap on a non existing map" should {
//      "create a new subMap" in {
//        val root = newDB()
//
//        val first = root.createMap(1, "first").assertGet
//        val second = first.putMap(2, "second").assertGet
//        first.getMap(2).assertGetOpt shouldBe defined
//        second.getMap(2).assertGetOpt shouldBe empty
//
//      }
//    }
//
//    "putMap on a existing map" should {
//      "replace existing map" in {
//        val root = newDB()
//
//        val first = root.createMap(1, "first").assertGet
//        val second = first.putMap(2, "second").assertGet
//        val secondAgain = first.putMap(2, "second again").assertGet
//
//        first.getMap(2).assertGetOpt shouldBe defined
//        first.getMapValue(2).assertGet shouldBe "second again"
//        second.getValue().assertGet shouldBe "second again"
//        secondAgain.getValue().assertGet shouldBe "second again"
//      }
//
//      "replace existing map and all it's entries" in {
//        val root = newDB()
//
//        val first = root.createMap(1, "first").assertGet
//        val second = first.putMap(2, "second").assertGet
//        //write entries to second map
//        second.put(1, "one").assertGet
//        second.put(2, "two").assertGet
//        second.put(3, "three").assertGet
//        //assert second map has these entries
//        second.toList shouldBe List((1, "one"), (2, "two"), (3, "three"))
//
//        val secondAgain = first.putMap(2, "second again").assertGet
//
//        //map value get updated
//        first.getMap(2).assertGetOpt shouldBe defined
//        first.getMapValue(2).assertGet shouldBe "second again"
//        second.getValue().assertGet shouldBe "second again"
//        secondAgain.getValue().assertGet shouldBe "second again"
//        //all the old entries are removed
//        second.includeSubMaps().toList shouldBe empty
//      }
//
//      "replace existing map and all it's entries and also all existing maps subMap and all their entries" in {
//        val root = newDB()
//
//        //MAP HIERARCHY
//        //first
//        //   second
//        //       third
//        //           fourth
//        val first = root.createMap(1, "first").assertGet
//        val second = first.putMap(2, "second").assertGet
//        second.put(1, "second one").assertGet
//        second.put(2, "second two").assertGet
//        second.put(3, "second three").assertGet
//        //third map that is the child map of second map
//        val third = second.putMap(3, "third").assertGet
//        third.put(1, "third one").assertGet
//        third.put(2, "third two").assertGet
//        third.put(3, "third three").assertGet
//        val fourth = third.putMap(4, "fourth").assertGet
//        fourth.put(1, "fourth one").assertGet
//        fourth.put(2, "fourth two").assertGet
//        fourth.put(3, "fourth three").assertGet
//
//        /**
//          * Assert that the all maps' content is accurate
//          */
//        second.toList shouldBe List((1, "second one"), (2, "second two"), (3, "second three"))
//        third.toList shouldBe List((1, "third one"), (2, "third two"), (3, "third three"))
//        fourth.toList shouldBe List((1, "fourth one"), (2, "fourth two"), (3, "fourth three"))
//
//        second.includeSubMaps().toList shouldBe List((1, "second one"), (2, "second two"), (3, "second three"), (3, "third"))
//        third.includeSubMaps().toList shouldBe List((1, "third one"), (2, "third two"), (3, "third three"), (4, "fourth"))
//        fourth.includeSubMaps().toList shouldBe List((1, "fourth one"), (2, "fourth two"), (3, "fourth three"))
//
//        second.subMapsOnly().toList shouldBe List((3, "third"))
//        third.subMapsOnly().toList shouldBe List((4, "fourth"))
//        fourth.subMapsOnly().toList shouldBe empty
//
//        //submit put on second map and assert that all it's contents are replaced.
//        val secondAgain = first.putMap(2, "second updated").assertGet
//
//        //map value get updated
//        first.getMap(2).assertGetOpt shouldBe defined
//        first.getMapValue(2).assertGet shouldBe "second updated"
//        second.getValue().assertGet shouldBe "second updated"
//        secondAgain.getValue().assertGet shouldBe "second updated"
//        //all the old entries are removed
//        second.includeSubMaps().toList shouldBe empty
//        third.includeSubMaps().toList shouldBe empty
//        fourth.includeSubMaps().toList shouldBe empty
//
//        second.containsMap(3).assertGet shouldBe false
//        second.containsMap(4).assertGet shouldBe false
//      }
//    }
//
//    "updateMapValue on an existing map" should {
//      "should update the maps value without remov" in {
//
//      }
//    }
//
//  }
//}