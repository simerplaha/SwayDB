///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
//import scala.concurrent.duration._
//import swaydb.{Prepare, TestBaseEmbedded}
//import swaydb.core.CommonAssertions._
//import swaydb.core.IOAssert._
//import swaydb.core.RunThis._
//import swaydb.core.TestBase
//import swaydb.data.order.KeyOrder
//import swaydb.data.slice.Slice
//import swaydb.data.util.StorageUnits._
//import swaydb.serializers.Default._
//
//class MapSpec0 extends MapSpec {
//  val keyValueCount: Int = 1000
//
//  override def newDB(): Map[Int, String] =
//    swaydb.persistent.Map[Key[Int], Option[String]](dir = randomDir).assertGet.extend.assertGet
//}
//
//class MapSpec1 extends MapSpec {
//
//  val keyValueCount: Int = 10000
//
//  override def newDB(): Map[Int, String] =
//    swaydb.persistent.Map[Key[Int], Option[String]](randomDir, mapSize = 1.byte).assertGet.extend.assertGet
//}
//
//class MapSpec2 extends MapSpec {
//
//  val keyValueCount: Int = 100000
//
//  override def newDB(): Map[Int, String] =
//    swaydb.memory.Map[Key[Int], Option[String]](mapSize = 1.byte).assertGet.extend.assertGet
//}
//
//class MapSpec3 extends MapSpec {
//  val keyValueCount: Int = 100000
//
//  override def newDB(): Map[Int, String] =
//    swaydb.memory.Map[Key[Int], Option[String]]().assertGet.extend.assertGet
//}
//
//sealed trait MapSpec extends TestBase with TestBaseEmbedded {
//
//  val keyValueCount: Int
//
//  def newDB(): Map[Int, String]
//
//  implicit val mapKeySerializer = Key.serializer(IntSerializer)
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//
//  "Extend" should {
//    "initialise a rootMap" in {
//      val rootMap = newDB()
//
//      rootMap shouldBe empty
//
//      //assert
//      rootMap.baseMap().toList shouldBe
//        List(
//          (Key.MapStart(Seq.empty), None),
//          (Key.MapEntriesStart(Seq.empty), None),
//          (Key.MapEntriesEnd(Seq.empty), None),
//          (Key.SubMapsStart(Seq.empty), None),
//          (Key.SubMapsEnd(Seq.empty), None),
//          (Key.MapEnd(Seq.empty), None)
//        )
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "update a rootMaps value" in {
//      val rootMap = newDB()
//
//      rootMap.getValue().assertGetOpt shouldBe empty
//      rootMap.updateValue("rootMap").assertGet
//      rootMap.getValue().assertGet shouldBe "rootMap"
//
//      //assert
//      rootMap.baseMap().toList shouldBe
//        List(
//          (Key.MapStart(Seq.empty), Some("rootMap")),
//          (Key.MapEntriesStart(Seq.empty), None),
//          (Key.MapEntriesEnd(Seq.empty), None),
//          (Key.SubMapsStart(Seq.empty), None),
//          (Key.SubMapsEnd(Seq.empty), None),
//          (Key.MapEnd(Seq.empty), None)
//        )
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "insert key-values to rootMap" in {
//      val rootMap = newDB()
//      rootMap.put(1, "one").assertGet
//      rootMap.put(2, "two").assertGet
//
//      rootMap.toList shouldBe List((1, "one"), (2, "two"))
//
//      //assert
//      rootMap.baseMap().toList shouldBe
//        List(
//          (Key.MapStart(Seq.empty), None),
//          (Key.MapEntriesStart(Seq.empty), None),
//          (Key.MapEntry(Seq.empty, 1), Some("one")),
//          (Key.MapEntry(Seq.empty, 2), Some("two")),
//          (Key.MapEntriesEnd(Seq.empty), None),
//          (Key.SubMapsStart(Seq.empty), None),
//          (Key.SubMapsEnd(Seq.empty), None),
//          (Key.MapEnd(Seq.empty), None)
//        )
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "insert a subMap" in {
//      val rootMap = newDB()
//      rootMap.put(1, "one").assertGet
//      rootMap.put(2, "two").assertGet
//
//      rootMap.maps.unsafeGet(1).assertGetOpt shouldBe empty
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//
//      rootMap.maps.unsafeGet(1).assertGetOpt shouldBe defined
//
//      subMap.put(1, "subMap one").assertGet
//      subMap.put(2, "subMap two").assertGet
//
//      rootMap.toList shouldBe List((1, "one"), (2, "two"))
//      subMap.toList shouldBe List((1, "subMap one"), (2, "subMap two"))
//
//      //assert
//      rootMap.baseMap().toList shouldBe
//        List(
//          (Key.MapStart(Seq.empty), None),
//          (Key.MapEntriesStart(Seq.empty), None),
//          (Key.MapEntry(Seq.empty, 1), Some("one")),
//          (Key.MapEntry(Seq.empty, 2), Some("two")),
//          (Key.MapEntriesEnd(Seq.empty), None),
//          (Key.SubMapsStart(Seq.empty), None),
//          (Key.SubMap(Seq.empty, 1), Some("sub map")),
//          (Key.SubMapsEnd(Seq.empty), None),
//          (Key.MapEnd(Seq.empty), None),
//
//          //subMaps entries
//          (Key.MapStart(Seq(1)), Some("sub map")),
//          (Key.MapEntriesStart(Seq(1)), None),
//          (Key.MapEntry(Seq(1), 1), Some("subMap one")),
//          (Key.MapEntry(Seq(1), 2), Some("subMap two")),
//          (Key.MapEntriesEnd(Seq(1)), None),
//          (Key.SubMapsStart(Seq(1)), None),
//          (Key.SubMapsEnd(Seq(1)), None),
//          (Key.MapEnd(Seq(1)), None)
//        )
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "remove all entries from rootMap and subMap" in {
//      val rootMap = newDB()
//      rootMap.put(1, "one").assertGet
//      rootMap.put(2, "two").assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//
//      subMap.put(1, "subMap one").assertGet
//      subMap.put(2, "subMap two").assertGet
//
//      eitherOne(
//        left = {
//          rootMap.clear().assertGet
//          subMap.clear().assertGet
//        },
//        right = {
//          rootMap.remove(1, 2).assertGet
//          subMap.remove(1, 2).assertGet
//        }
//      )
//      //assert
////      rootMap.baseMap().toList shouldBe
////        List(
////          (Key.Start(Seq.empty), None),
////          (Key.EntriesStart(Seq.empty), None),
////          (Key.EntriesEnd(Seq.empty), None),
////          (Key.SubMapsStart(Seq.empty), None),
////          (Key.SubMap(Seq.empty, 1), Some("sub map")),
////          (Key.SubMapsEnd(Seq.empty), None),
////          (Key.End(Seq.empty), None),
////
////          //subMaps entries
////          (Key.Start(Seq(1)), Some("sub map")),
////          (Key.EntriesStart(Seq(1)), None),
////          (Key.EntriesEnd(Seq(1)), None),
////          (Key.SubMapsStart(Seq(1)), None),
////          (Key.SubMapsEnd(Seq(1)), None),
////          (Key.End(Seq(1)), None)
////        )
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "update a subMap's value" in {
//      val rootMap = newDB()
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      rootMap.maps.updateValue(1, "sub map updated")
//      rootMap.maps.contains(1).assertGet shouldBe true
//
//      //assert
////      rootMap.baseMap().toList shouldBe
////        List(
////          (Key.Start(Seq.empty), None),
////          (Key.EntriesStart(Seq.empty), None),
////          (Key.EntriesEnd(Seq.empty), None),
////          (Key.SubMapsStart(Seq.empty), None),
////          (Key.SubMap(Seq.empty, 1), Some("sub map updated")),
////          (Key.SubMapsEnd(Seq.empty), None),
////          (Key.End(Seq.empty), None),
////
////          //subMaps entries
////          (Key.Start(Seq(1)), Some("sub map updated")),
////          (Key.EntriesStart(Seq(1)), None),
////          (Key.EntriesEnd(Seq(1)), None),
////          (Key.SubMapsStart(Seq(1)), None),
////          (Key.SubMapsEnd(Seq(1)), None),
////          (Key.End(Seq(1)), None)
////        )
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "getMap, containsMap, exists & getMapValue" in {
//      val rootMap = newDB()
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      subMap.put(1, "one").assertGet
//      subMap.put(2, "two").assertGet
//
//      val subMapGet = rootMap.maps.unsafeGet(1).assertGet
//      subMapGet.getValue().assertGet shouldBe "sub map"
//      subMapGet.toList shouldBe List((1, "one"), (2, "two"))
//
//      rootMap.maps.contains(1).assertGet shouldBe true
//      rootMap.exists().assertGet shouldBe true
//      subMap.exists().assertGet shouldBe true
//      rootMap.maps.getValue(1).assertGet shouldBe "sub map"
//      rootMap.maps.getValue(2).assertGetOpt shouldBe empty //2 does not exists
//
//      rootMap.maps.remove(1).assertGet
//
//      rootMap.maps.contains(1).assertGet shouldBe false
//      rootMap.exists().assertGet shouldBe true
//      subMap.exists().assertGet shouldBe false
//      rootMap.maps.getValue(1).assertGetOpt shouldBe empty //is deleted
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "expire key" in {
//      val rootMap = newDB()
//      rootMap.put(1, "one", 500.millisecond).assertGet
//      rootMap.put(2, "two").assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//
//      subMap.put(1, "subMap one", 500.millisecond).assertGet
//      subMap.put(2, "subMap two").assertGet
//
//      eventual {
//        rootMap.unsafeGet(1).assertGetOpt shouldBe empty
//        subMap.unsafeGet(1).assertGetOpt shouldBe empty
//      }
//
//      //assert
//      //      rootMap.baseMap().toList shouldBe
//      //        List(
//      //          (Key.Start(Seq.empty), None),
//      //          (Key.EntriesStart(Seq.empty), None),
//      //          //          (Key.Entry(Seq.empty, 1), Some("one")),//expired
//      //          (Key.Entry(Seq.empty, 2), Some("two")),
//      //          (Key.EntriesEnd(Seq.empty), None),
//      //          (Key.SubMapsStart(Seq.empty), None),
//      //          (Key.SubMap(Seq.empty, 1), Some("sub map")),
//      //          (Key.SubMapsEnd(Seq.empty), None),
//      //          (Key.End(Seq.empty), None),
//      //
//      //          //subMaps entries
//      //          (Key.Start(Seq(1)), Some("sub map")),
//      //          (Key.EntriesStart(Seq(1)), None),
//      //          //          (Key.Entry(Seq(1), 1), Some("subMap one")), //expired
//      //          (Key.Entry(Seq(1), 2), Some("subMap two")),
//      //          (Key.EntriesEnd(Seq(1)), None),
//      //          (Key.SubMapsStart(Seq(1)), None),
//      //          (Key.SubMapsEnd(Seq(1)), None),
//      //          (Key.End(Seq(1)), None)
//      //        )
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "expire range keys" in {
//      val rootMap = newDB()
//      rootMap.put(1, "one").assertGet
//      rootMap.put(2, "two").assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//
//      subMap.put(1, "subMap two").assertGet
//      subMap.put(2, "subMap two").assertGet
//      subMap.put(3, "subMap two").assertGet
//      subMap.put(4, "subMap two").assertGet
//
//      rootMap.expire(1, 2, 100.millisecond).assertGet //expire all key-values from rootMap
//      subMap.expire(2, 3, 100.millisecond).assertGet //expire some from subMap
//
//      eventual {
//        rootMap.unsafeGet(1).assertGetOpt shouldBe empty
//        rootMap.unsafeGet(2).assertGetOpt shouldBe empty
//        subMap.unsafeGet(1).assertGet shouldBe "subMap two"
//        subMap.unsafeGet(2).assertGetOpt shouldBe empty
//        subMap.unsafeGet(3).assertGetOpt shouldBe empty
//        subMap.unsafeGet(4).assertGet shouldBe "subMap two"
//      }
//
//      //assert
//      //      rootMap.baseMap().toList shouldBe
//      //        List(
//      //          (Key.Start(Seq.empty), None),
//      //          (Key.EntriesStart(Seq.empty), None),
//      //          (Key.EntriesEnd(Seq.empty), None),
//      //          (Key.SubMapsStart(Seq.empty), None),
//      //          (Key.SubMap(Seq.empty, 1), Some("sub map")),
//      //          (Key.SubMapsEnd(Seq.empty), None),
//      //          (Key.End(Seq.empty), None),
//      //
//      //          //subMaps entries
//      //          (Key.Start(Seq(1)), Some("sub map")),
//      //          (Key.EntriesStart(Seq(1)), None),
//      //          (Key.Entry(Seq(1), 1), Some("subMap two")),
//      //          (Key.Entry(Seq(1), 4), Some("subMap two")),
//      //          (Key.EntriesEnd(Seq(1)), None),
//      //          (Key.SubMapsStart(Seq(1)), None),
//      //          (Key.SubMapsEnd(Seq(1)), None),
//      //          (Key.End(Seq(1)), None)
//      //        )
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "update range keys" in {
//      val rootMap = newDB()
//      rootMap.put(1, "one").assertGet
//      rootMap.put(2, "two").assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//
//      subMap.put(1, "subMap two").assertGet
//      subMap.put(2, "subMap two").assertGet
//      subMap.put(3, "subMap two").assertGet
//      subMap.put(4, "subMap two").assertGet
//
//      eitherOne(
//        left = {
//          rootMap.update(1, 2, "updated").assertGet //update all key-values from rootMap
//          subMap.update(2, 3, "updated").assertGet //update some from subMap
//        },
//        right = {
//          rootMap.update(1, "updated").assertGet
//          rootMap.update(2, "updated").assertGet
//          subMap.update(2, "updated").assertGet
//          subMap.update(3, "updated").assertGet
//        }
//      )
//
//      rootMap.unsafeGet(1).assertGet shouldBe "updated"
//      rootMap.unsafeGet(2).assertGet shouldBe "updated"
//      subMap.unsafeGet(2).assertGet shouldBe "updated"
//      subMap.unsafeGet(3).assertGet shouldBe "updated"
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "batch put" in {
//      val rootMap = newDB()
//      rootMap.commitPrepared(
//        Prepare.Put(1, "one"),
//        Prepare.Put(2, "two")
//      ).assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      subMap.commitPrepared(
//        Prepare.Put(1, "one one"),
//        Prepare.Put(2, "two two")
//      ).assertGet
//
//      rootMap.unsafeGet(1).assertGet shouldBe "one"
//      rootMap.unsafeGet(2).assertGet shouldBe "two"
//      subMap.unsafeGet(1).assertGet shouldBe "one one"
//      subMap.unsafeGet(2).assertGet shouldBe "two two"
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "batch update" in {
//      val rootMap = newDB()
//      rootMap.commitPrepared(
//        Prepare.Put(1, "one"),
//        Prepare.Put(2, "two")
//      ).assertGet
//
//      rootMap.commitPrepared(
//        Prepare.Update(1, "one updated"),
//        Prepare.Update(2, "two updated")
//      ).assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      subMap.commitPrepared(
//        Prepare.Put(1, "one one"),
//        Prepare.Put(2, "two two")
//      ).assertGet
//
//      subMap.commitPrepared(
//        Prepare.Update(1, "one one updated"),
//        Prepare.Update(2, "two two updated")
//      ).assertGet
//
//      rootMap.unsafeGet(1).assertGet shouldBe "one updated"
//      rootMap.unsafeGet(2).assertGet shouldBe "two updated"
//      subMap.unsafeGet(1).assertGet shouldBe "one one updated"
//      subMap.unsafeGet(2).assertGet shouldBe "two two updated"
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "batch expire" in {
//      val rootMap = newDB()
//      rootMap.commitPrepared(
//        Prepare.Put(1, "one"),
//        Prepare.Put(2, "two")
//      ).assertGet
//
//      rootMap.commitPrepared(
//        Prepare.Expire(1, 100.millisecond),
//        Prepare.Expire(2, 100.millisecond)
//      ).assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      subMap.commitPrepared(
//        Prepare.Put(1, "one one"),
//        Prepare.Put(2, "two two")
//      ).assertGet
//
//      subMap.commitPrepared(
//        Prepare.Expire(1, 100.millisecond),
//        Prepare.Expire(2, 100.millisecond)
//      ).assertGet
//
//      eventual {
//        rootMap shouldBe empty
//        subMap shouldBe empty
//      }
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "batchPut" in {
//      val rootMap = newDB()
//      rootMap.put((1, "one"), (2, "two")).assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      subMap.put((1, "one one"), (2, "two two"))
//
//      rootMap.toList shouldBe List((1, "one"), (2, "two"))
//      subMap.toList shouldBe List((1, "one one"), (2, "two two"))
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "batchUpdate" in {
//      val rootMap = newDB()
//      rootMap.put((1, "one"), (2, "two")).assertGet
//      rootMap.update((1, "one updated"), (2, "two updated")).assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      subMap.put((1, "one one"), (2, "two two"))
//      subMap.update((1, "one one updated"), (2, "two two updated")).assertGet
//
//      rootMap.toList shouldBe List((1, "one updated"), (2, "two updated"))
//      subMap.toList shouldBe List((1, "one one updated"), (2, "two two updated"))
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "batchRemove" in {
//      val rootMap = newDB()
//      rootMap.put((1, "one"), (2, "two")).assertGet
//      rootMap.remove(1, 2).assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      subMap.put((1, "one one"), (2, "two two"))
//      subMap.remove(1, 2).assertGet
//
//      rootMap.toList shouldBe empty
//      subMap.toList shouldBe empty
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "batchExpire" in {
//      val rootMap = newDB()
//      rootMap.put((1, "one"), (2, "two")).assertGet
//      rootMap.expire((1, 1.second.fromNow)).assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      subMap.put((1, "one one"), (2, "two two"))
//      subMap.expire((1, 1.second.fromNow), (2, 1.second.fromNow)).assertGet
//
//      eventual {
//        rootMap.toList should contain only ((2, "two"))
//        subMap.toList shouldBe empty
//      }
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "unsafeGet" in {
//      val rootMap = newDB()
//      rootMap.put((1, "one"), (2, "two")).assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      subMap.put((1, "one one"), (2, "two two"))
//
//      rootMap.unsafeGet(1).assertGet shouldBe "one"
//      rootMap.unsafeGet(2).assertGet shouldBe "two"
//      subMap.unsafeGet(1).assertGet shouldBe "one one"
//      subMap.unsafeGet(2).assertGet shouldBe "two two"
//
//      rootMap.remove(1, 2).assertGet
//      subMap.remove(1, 2).assertGet
//
//      rootMap.unsafeGet(1).assertGetOpt shouldBe empty
//      rootMap.unsafeGet(2).assertGetOpt shouldBe empty
//      subMap.unsafeGet(1).assertGetOpt shouldBe empty
//      subMap.unsafeGet(2).assertGetOpt shouldBe empty
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "unsafeGet when sub map is removed" in {
//      val rootMap = newDB()
//      rootMap.put((1, "one"), (2, "two")).assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      subMap.put((1, "one one"), (2, "two two"))
//
//      rootMap.unsafeGet(1).assertGet shouldBe "one"
//      rootMap.unsafeGet(2).assertGet shouldBe "two"
//      subMap.unsafeGet(1).assertGet shouldBe "one one"
//      subMap.unsafeGet(2).assertGet shouldBe "two two"
//
//      rootMap.remove(1, 2).assertGet
//      rootMap.maps.remove(1).assertGet
//
//      rootMap.unsafeGet(1).assertGetOpt shouldBe empty
//      rootMap.unsafeGet(2).assertGetOpt shouldBe empty
//      subMap.unsafeGet(1).assertGetOpt shouldBe empty
//      subMap.unsafeGet(2).assertGetOpt shouldBe empty
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "getKey" in {
//      val rootMap = newDB()
//      rootMap.put((1, "one"), (2, "two")).assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      subMap.put((11, "one one"), (22, "two two"))
//
//      rootMap.getKey(1).assertGet shouldBe 1
//      rootMap.getKey(2).assertGet shouldBe 2
//      subMap.getKey(11).assertGet shouldBe 11
//      subMap.getKey(22).assertGet shouldBe 22
//
//      rootMap.remove(1, 2).assertGet
//      rootMap.maps.remove(1).assertGet
//
//      rootMap.unsafeGet(1).assertGetOpt shouldBe empty
//      rootMap.unsafeGet(2).assertGetOpt shouldBe empty
//      subMap.unsafeGet(11).assertGetOpt shouldBe empty
//      subMap.unsafeGet(22).assertGetOpt shouldBe empty
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "getKeyValue" in {
//      val rootMap = newDB()
//      rootMap.put((1, "one"), (2, "two")).assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      subMap.put((11, "one one"), (22, "two two"))
//
//      rootMap.getKeyValue(1).assertGet shouldBe(1, "one")
//      rootMap.getKeyValue(2).assertGet shouldBe(2, "two")
//      subMap.getKeyValue(11).assertGet shouldBe(11, "one one")
//      subMap.getKeyValue(22).assertGet shouldBe(22, "two two")
//
//      rootMap.remove(1, 2).assertGet
//      rootMap.maps.remove(1).assertGet
//
//      rootMap.getKeyValue(1).assertGetOpt shouldBe empty
//      rootMap.getKeyValue(2).assertGetOpt shouldBe empty
//      subMap.getKeyValue(11).assertGetOpt shouldBe empty
//      subMap.getKeyValue(22).assertGetOpt shouldBe empty
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "keys" in {
//      val rootMap = newDB()
//      rootMap.put((1, "one"), (2, "two")).assertGet
//
//      val subMap = rootMap.maps.put(1, "sub map").assertGet
//      subMap.put((11, "one one"), (22, "two two"))
//
//      rootMap.keys.toList should contain inOrderOnly(1, 2)
//      subMap.keys.toList should contain inOrderOnly(11, 22)
//
//      rootMap.closeDatabase().unsafeGet
//    }
//  }
//
//  "Map" should {
//
//    "return entries ranges" in {
//      Map.entriesRangeKeys(Seq(1, 2, 3)) shouldBe ((Key.MapEntriesStart(Seq(1, 2, 3)), Key.MapEntriesEnd(Seq(1, 2, 3))))
//    }
//
//    "return empty subMap range keys for a empty SubMap" in {
//      val db = newDB()
//
//      val rootMap = db.maps.put(1, "rootMap").assertGet
//      Map.childSubMapRanges(rootMap) shouldBe empty
//
//      db.closeDatabase().unsafeGet
//    }
//
//    "return subMap that has only one child subMap" in {
//      val rootMap = newDB()
//
//      val firstMap = rootMap.maps.put(1, "rootMap").assertGet
//      val secondMap = firstMap.maps.put(2, "second map").assertGet
//
//      Map.childSubMapRanges(firstMap) should contain only ((Key.SubMap(Seq(1), 2), Key.MapStart(Seq(1, 2)), Key.MapEnd(Seq(1, 2))))
//      Map.childSubMapRanges(secondMap) shouldBe empty
//
//      rootMap.closeDatabase().unsafeGet
//    }
//
//    "return subMaps of 3 nested maps" in {
//      val db = newDB()
//
//      val firstMap = db.maps.put(1, "first").assertGet
//      val secondMap = firstMap.maps.put(2, "second").assertGet
//      val thirdMap = secondMap.maps.put(2, "third").assertGet
//
//      Map.childSubMapRanges(firstMap) should contain inOrderOnly((Key.SubMap(Seq(1), 2), Key.MapStart(Seq(1, 2)), Key.MapEnd(Seq(1, 2))), (Key.SubMap(Seq(1, 2), 2), Key.MapStart(Seq(1, 2, 2)), Key.MapEnd(Seq(1, 2, 2))))
//      Map.childSubMapRanges(secondMap) should contain only ((Key.SubMap(Seq(1, 2), 2), Key.MapStart(Seq(1, 2, 2)), Key.MapEnd(Seq(1, 2, 2))))
//      Map.childSubMapRanges(thirdMap) shouldBe empty
//
//      db.closeDatabase().unsafeGet
//    }
//
//    "returns multiple child subMap that also contains nested subMaps" in {
//      val db = newDB()
//
//      val firstMap = db.maps.put(1, "firstMap").assertGet
//      val secondMap = firstMap.maps.put(2, "subMap").assertGet
//
//      secondMap.maps.put(2, "subMap").assertGet
//      secondMap.maps.put(3, "subMap3").assertGet
//      val subMap4 = secondMap.maps.put(4, "subMap4").assertGet
//      subMap4.maps.put(44, "subMap44").assertGet
//      val subMap5 = secondMap.maps.put(5, "subMap5").assertGet
//      val subMap55 = subMap5.maps.put(55, "subMap55").assertGet
//      subMap55.maps.put(5555, "subMap55").assertGet
//      subMap55.maps.put(6666, "subMap55").assertGet
//      subMap5.maps.put(555, "subMap555").assertGet
//
//      val mapHierarchy =
//        List(
//          (Key.SubMap(Seq(1), 2), Key.MapStart(Seq(1, 2)), Key.MapEnd(Seq(1, 2))),
//          (Key.SubMap(Seq(1, 2), 2), Key.MapStart(Seq(1, 2, 2)), Key.MapEnd(Seq(1, 2, 2))),
//          (Key.SubMap(Seq(1, 2), 3), Key.MapStart(Seq(1, 2, 3)), Key.MapEnd(Seq(1, 2, 3))),
//          (Key.SubMap(Seq(1, 2), 4), Key.MapStart(Seq(1, 2, 4)), Key.MapEnd(Seq(1, 2, 4))),
//          (Key.SubMap(Seq(1, 2, 4), 44), Key.MapStart(Seq(1, 2, 4, 44)), Key.MapEnd(Seq(1, 2, 4, 44))),
//          (Key.SubMap(Seq(1, 2), 5), Key.MapStart(Seq(1, 2, 5)), Key.MapEnd(Seq(1, 2, 5))),
//          (Key.SubMap(Seq(1, 2, 5), 55), Key.MapStart(Seq(1, 2, 5, 55)), Key.MapEnd(Seq(1, 2, 5, 55))),
//          (Key.SubMap(Seq(1, 2, 5, 55), 5555), Key.MapStart(Seq(1, 2, 5, 55, 5555)), Key.MapEnd(Seq(1, 2, 5, 55, 5555))),
//          (Key.SubMap(Seq(1, 2, 5, 55), 6666), Key.MapStart(Seq(1, 2, 5, 55, 6666)), Key.MapEnd(Seq(1, 2, 5, 55, 6666))),
//          (Key.SubMap(Seq(1, 2, 5), 555), Key.MapStart(Seq(1, 2, 5, 555)), Key.MapEnd(Seq(1, 2, 5, 555)))
//        )
//
//      Map.childSubMapRanges(firstMap) shouldBe mapHierarchy
//      Map.childSubMapRanges(secondMap) shouldBe mapHierarchy.drop(1)
//
//      db.closeDatabase().unsafeGet
//    }
//  }
//
//  "SubMap" when {
//    "maps.put on a non existing map" should {
//      "create a new subMap" in {
//        val root = newDB()
//
//        val first = root.maps.put(1, "first").assertGet
//        val second = first.maps.put(2, "second").assertGet
//        first.maps.unsafeGet(2).assertGetOpt shouldBe defined
//        second.maps.unsafeGet(2).assertGetOpt shouldBe empty
//
//        root.closeDatabase().unsafeGet
//      }
//    }
//
//    "maps.put on a existing map" should {
//      "replace existing map" in {
//        val root = newDB()
//
//        val first = root.maps.put(1, "first").assertGet
//        val second = first.maps.put(2, "second").assertGet
//        val secondAgain = first.maps.put(2, "second again").assertGet
//
//        first.maps.unsafeGet(2).assertGetOpt shouldBe defined
//        first.maps.getValue(2).assertGet shouldBe "second again"
//        second.getValue().assertGet shouldBe "second again"
//        secondAgain.getValue().assertGet shouldBe "second again"
//
//        root.closeDatabase().unsafeGet
//      }
//
//      "replace existing map and all it's entries" in {
//        val root = newDB()
//
//        val first = root.maps.put(1, "first").assertGet
//        val second = first.maps.put(2, "second").assertGet
//        //write entries to second map
//        second.put(1, "one").assertGet
//        second.put(2, "two").assertGet
//        second.put(3, "three").assertGet
//        //assert second map has these entries
//        second.toList shouldBe List((1, "one"), (2, "two"), (3, "three"))
//
//        val secondAgain = first.maps.put(2, "second again").assertGet
//
//        //map value unsafeGet updated
//        first.maps.unsafeGet(2).assertGetOpt shouldBe defined
//        first.maps.getValue(2).assertGet shouldBe "second again"
//        second.getValue().assertGet shouldBe "second again"
//        secondAgain.getValue().assertGet shouldBe "second again"
//        //all the old entries are removed
//        second.toList shouldBe empty
//
//        root.closeDatabase().unsafeGet
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
//        val first = root.maps.put(1, "first").assertGet
//        val second = first.maps.put(2, "second").assertGet
//        second.put(1, "second one").assertGet
//        second.put(2, "second two").assertGet
//        second.put(3, "second three").assertGet
//        //third map that is the child map of second map
//        val third = second.maps.put(3, "third").assertGet
//        third.put(1, "third one").assertGet
//        third.put(2, "third two").assertGet
//        third.put(3, "third three").assertGet
//        val fourth = third.maps.put(4, "fourth").assertGet
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
//        second.toList shouldBe List((1, "second one"), (2, "second two"), (3, "second three"))
//        third.toList shouldBe List((1, "third one"), (2, "third two"), (3, "third three"))
//        fourth.toList shouldBe List((1, "fourth one"), (2, "fourth two"), (3, "fourth three"))
//
//        second.maps.toList shouldBe List((3, "third"))
//        third.maps.toList shouldBe List((4, "fourth"))
//        fourth.maps.toList shouldBe empty
//
//        //submit put on second map and assert that all it's contents are replaced.
//        val secondAgain = first.maps.put(2, "second updated").assertGet
//
//        //map value unsafeGet updated
//        first.maps.unsafeGet(2).assertGetOpt shouldBe defined
//        first.maps.getValue(2).assertGet shouldBe "second updated"
//        second.getValue().assertGet shouldBe "second updated"
//        secondAgain.getValue().assertGet shouldBe "second updated"
//        //all the old entries are removed
//        second.toList shouldBe empty
//        third.toList shouldBe empty
//        fourth.toList shouldBe empty
//
//        second.maps.contains(3).assertGet shouldBe false
//        second.maps.contains(4).assertGet shouldBe false
//
//        root.closeDatabase().unsafeGet
//      }
//    }
//
//    "clear" should {
//      "remove all key-values from a map" in {
//
//        val root = newDB()
//        val first = root.maps.put(1, "first").assertGet
//        val second = first.maps.put(2, "second").assertGet
//        second.put(1, "second one").assertGet
//        second.put(2, "second two").assertGet
//        second.put(3, "second three").assertGet
//        //third map that is the child map of second map
//        val third = second.maps.put(3, "third").assertGet
//        third.put(1, "third one").assertGet
//        third.put(2, "third two").assertGet
//        third.put(3, "third three").assertGet
//
//        second should have size 3
//        second.clear().assertGet
//        second shouldBe empty
//
//        third should have size 3
//        second.maps.clear(3).assertGet
//        third shouldBe empty
//
//        root.closeDatabase().unsafeGet
//      }
//    }
//  }
//}
