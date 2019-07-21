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

import swaydb.Prepare
import swaydb.api.TestBaseEmbedded
import swaydb.core.CommonAssertions._
import swaydb.core.IOValues._
import swaydb.core.RunThis._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class MapSpec0 extends MapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.persistent.Map[Int, String](dir = randomDir).runIO
}

class MapSpec1 extends MapSpec {

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.persistent.Map[Int, String](randomDir, mapSize = 1.byte).runIO
}

class MapSpec2 extends MapSpec {

  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.memory.Map[Int, String](mapSize = 1.byte).runIO
}

class MapSpec3 extends MapSpec {
  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String] =
    swaydb.extensions.memory.Map[Int, String]().runIO
}

sealed trait MapSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): Map[Int, String]

  implicit val mapKeySerializer = Key.serializer(IntSerializer)
  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  "Extend" should {
    "initialise a rootMap" in {
      val rootMap = newDB()

      rootMap.stream.materialize.runIO shouldBe empty

      //assert
      rootMap.baseMap().stream.materialize.runIO shouldBe
        List(
          (Key.MapStart(Seq.empty), None),
          (Key.MapEntriesStart(Seq.empty), None),
          (Key.MapEntriesEnd(Seq.empty), None),
          (Key.SubMapsStart(Seq.empty), None),
          (Key.SubMapsEnd(Seq.empty), None),
          (Key.MapEnd(Seq.empty), None)
        )

      rootMap.closeDatabase().get
    }

    "update a rootMaps value" in {
      val rootMap = newDB()

      rootMap.getValue().runIO shouldBe empty
      rootMap.updateValue("rootMap").runIO
      rootMap.getValue().runIO.value shouldBe "rootMap"

      //assert
      rootMap.baseMap().stream.materialize.runIO shouldBe
        List(
          (Key.MapStart(Seq.empty), Some("rootMap")),
          (Key.MapEntriesStart(Seq.empty), None),
          (Key.MapEntriesEnd(Seq.empty), None),
          (Key.SubMapsStart(Seq.empty), None),
          (Key.SubMapsEnd(Seq.empty), None),
          (Key.MapEnd(Seq.empty), None)
        )

      rootMap.closeDatabase().get
    }

    "insert key-values to rootMap" in {
      val rootMap = newDB()
      rootMap.put(1, "one").runIO
      rootMap.put(2, "two").runIO

      rootMap.get(1).get.get shouldBe "one"
      rootMap.get(2).get.get shouldBe "two"

      rootMap.stream.materialize.runIO shouldBe ListBuffer((1, "one"), (2, "two"))

      //assert
      rootMap.baseMap().stream.materialize.runIO shouldBe
        List(
          (Key.MapStart(Seq.empty), None),
          (Key.MapEntriesStart(Seq.empty), None),
          (Key.MapEntry(Seq.empty, 1), Some("one")),
          (Key.MapEntry(Seq.empty, 2), Some("two")),
          (Key.MapEntriesEnd(Seq.empty), None),
          (Key.SubMapsStart(Seq.empty), None),
          (Key.SubMapsEnd(Seq.empty), None),
          (Key.MapEnd(Seq.empty), None)
        )

      rootMap.closeDatabase().get
    }

    "insert a subMap" in {
      val rootMap = newDB()
      rootMap.put(1, "one").runIO
      rootMap.put(2, "two").runIO

      rootMap.maps.get(1).runIO shouldBe empty

      val subMap = rootMap.maps.put(1, "sub map").runIO

      rootMap.maps.get(1).runIO shouldBe defined

      subMap.put(1, "subMap one").runIO
      subMap.put(2, "subMap two").runIO

      rootMap.stream.materialize.runIO shouldBe ListBuffer((1, "one"), (2, "two"))
      subMap.stream.materialize.runIO shouldBe ListBuffer((1, "subMap one"), (2, "subMap two"))

      //assert
      rootMap.baseMap().stream.materialize.runIO shouldBe
        List(
          (Key.MapStart(Seq.empty), None),
          (Key.MapEntriesStart(Seq.empty), None),
          (Key.MapEntry(Seq.empty, 1), Some("one")),
          (Key.MapEntry(Seq.empty, 2), Some("two")),
          (Key.MapEntriesEnd(Seq.empty), None),
          (Key.SubMapsStart(Seq.empty), None),
          (Key.SubMap(Seq.empty, 1), Some("sub map")),
          (Key.SubMapsEnd(Seq.empty), None),
          (Key.MapEnd(Seq.empty), None),

          //subMaps entries
          (Key.MapStart(Seq(1)), Some("sub map")),
          (Key.MapEntriesStart(Seq(1)), None),
          (Key.MapEntry(Seq(1), 1), Some("subMap one")),
          (Key.MapEntry(Seq(1), 2), Some("subMap two")),
          (Key.MapEntriesEnd(Seq(1)), None),
          (Key.SubMapsStart(Seq(1)), None),
          (Key.SubMapsEnd(Seq(1)), None),
          (Key.MapEnd(Seq(1)), None)
        )

      rootMap.closeDatabase().get
    }

    "remove all entries from rootMap and subMap" in {
      val rootMap = newDB()
      rootMap.put(1, "one").runIO
      rootMap.put(2, "two").runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO

      subMap.put(1, "subMap one").runIO
      subMap.put(2, "subMap two").runIO

      eitherOne(
        left = {
          rootMap.clear().runIO
          subMap.clear().runIO
        },
        right = {
          rootMap.remove(1, 2).runIO
          subMap.remove(1, 2).runIO
        }
      )
      //assert
      //      rootMap.baseMap().toList shouldBe
      //        List(
      //          (Key.Start(Seq.empty), None),
      //          (Key.EntriesStart(Seq.empty), None),
      //          (Key.EntriesEnd(Seq.empty), None),
      //          (Key.SubMapsStart(Seq.empty), None),
      //          (Key.SubMap(Seq.empty, 1), Some("sub map")),
      //          (Key.SubMapsEnd(Seq.empty), None),
      //          (Key.End(Seq.empty), None),
      //
      //          //subMaps entries
      //          (Key.Start(Seq(1)), Some("sub map")),
      //          (Key.EntriesStart(Seq(1)), None),
      //          (Key.EntriesEnd(Seq(1)), None),
      //          (Key.SubMapsStart(Seq(1)), None),
      //          (Key.SubMapsEnd(Seq(1)), None),
      //          (Key.End(Seq(1)), None)
      //        )

      rootMap.closeDatabase().get
    }

    "update a subMap's value" in {
      val rootMap = newDB()

      val subMap = rootMap.maps.put(1, "sub map").runIO
      rootMap.maps.updateValue(1, "sub map updated")
      rootMap.maps.contains(1).runIO shouldBe true

      //assert
      //      rootMap.baseMap().toList shouldBe
      //        List(
      //          (Key.Start(Seq.empty), None),
      //          (Key.EntriesStart(Seq.empty), None),
      //          (Key.EntriesEnd(Seq.empty), None),
      //          (Key.SubMapsStart(Seq.empty), None),
      //          (Key.SubMap(Seq.empty, 1), Some("sub map updated")),
      //          (Key.SubMapsEnd(Seq.empty), None),
      //          (Key.End(Seq.empty), None),
      //
      //          //subMaps entries
      //          (Key.Start(Seq(1)), Some("sub map updated")),
      //          (Key.EntriesStart(Seq(1)), None),
      //          (Key.EntriesEnd(Seq(1)), None),
      //          (Key.SubMapsStart(Seq(1)), None),
      //          (Key.SubMapsEnd(Seq(1)), None),
      //          (Key.End(Seq(1)), None)
      //        )

      rootMap.closeDatabase().get
    }

    "getMap, containsMap, exists & getMapValue" in {
      val rootMap = newDB()

      val subMap = rootMap.maps.put(1, "sub map").runIO
      subMap.put(1, "one").runIO
      subMap.put(2, "two").runIO

      val subMapGet = rootMap.maps.get(1).runIO.value
      subMapGet.getValue().runIO.value shouldBe "sub map"
      subMapGet.stream.materialize.runIO shouldBe ListBuffer((1, "one"), (2, "two"))

      rootMap.maps.contains(1).runIO shouldBe true
      rootMap.exists().runIO shouldBe true
      subMap.exists().runIO shouldBe true
      rootMap.maps.getValue(1).runIO.value shouldBe "sub map"
      rootMap.maps.getValue(2).runIO shouldBe empty //2 does not exists

      rootMap.maps.remove(1).runIO

      rootMap.maps.contains(1).runIO shouldBe false
      rootMap.exists().runIO shouldBe true
      subMap.exists().runIO shouldBe false
      rootMap.maps.getValue(1).runIO shouldBe empty //is deleted

      rootMap.closeDatabase().get
    }

    "expire key" in {
      val rootMap = newDB()
      rootMap.put(1, "one", 500.millisecond).runIO
      rootMap.put(2, "two").runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO

      subMap.put(1, "subMap one", 500.millisecond).runIO
      subMap.put(2, "subMap two").runIO

      eventual {
        rootMap.get(1).runIO shouldBe empty
        subMap.get(1).assertGetOpt shouldBe empty
      }

      //assert
      //      rootMap.baseMap().toList shouldBe
      //        List(
      //          (Key.Start(Seq.empty), None),
      //          (Key.EntriesStart(Seq.empty), None),
      //          //          (Key.Entry(Seq.empty, 1), Some("one")),//expired
      //          (Key.Entry(Seq.empty, 2), Some("two")),
      //          (Key.EntriesEnd(Seq.empty), None),
      //          (Key.SubMapsStart(Seq.empty), None),
      //          (Key.SubMap(Seq.empty, 1), Some("sub map")),
      //          (Key.SubMapsEnd(Seq.empty), None),
      //          (Key.End(Seq.empty), None),
      //
      //          //subMaps entries
      //          (Key.Start(Seq(1)), Some("sub map")),
      //          (Key.EntriesStart(Seq(1)), None),
      //          //          (Key.Entry(Seq(1), 1), Some("subMap one")), //expired
      //          (Key.Entry(Seq(1), 2), Some("subMap two")),
      //          (Key.EntriesEnd(Seq(1)), None),
      //          (Key.SubMapsStart(Seq(1)), None),
      //          (Key.SubMapsEnd(Seq(1)), None),
      //          (Key.End(Seq(1)), None)
      //        )

      rootMap.closeDatabase().get
    }

    "expire range keys" in {
      val rootMap = newDB()
      rootMap.put(1, "one").runIO
      rootMap.put(2, "two").runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO

      subMap.put(1, "subMap two").runIO
      subMap.put(2, "subMap two").runIO
      subMap.put(3, "subMap two").runIO
      subMap.put(4, "subMap two").runIO

      rootMap.expire(1, 2, 100.millisecond).runIO //expire all key-values from rootMap
      subMap.expire(2, 3, 100.millisecond).runIO //expire some from subMap

      eventual {
        rootMap.get(1).runIO shouldBe empty
        rootMap.get(2).runIO shouldBe empty
        subMap.get(1).assertGet shouldBe "subMap two"
        subMap.get(2).assertGetOpt shouldBe empty
        subMap.get(3).assertGetOpt shouldBe empty
        subMap.get(4).assertGet shouldBe "subMap two"
      }

      //assert
      //      rootMap.baseMap().toList shouldBe
      //        List(
      //          (Key.Start(Seq.empty), None),
      //          (Key.EntriesStart(Seq.empty), None),
      //          (Key.EntriesEnd(Seq.empty), None),
      //          (Key.SubMapsStart(Seq.empty), None),
      //          (Key.SubMap(Seq.empty, 1), Some("sub map")),
      //          (Key.SubMapsEnd(Seq.empty), None),
      //          (Key.End(Seq.empty), None),
      //
      //          //subMaps entries
      //          (Key.Start(Seq(1)), Some("sub map")),
      //          (Key.EntriesStart(Seq(1)), None),
      //          (Key.Entry(Seq(1), 1), Some("subMap two")),
      //          (Key.Entry(Seq(1), 4), Some("subMap two")),
      //          (Key.EntriesEnd(Seq(1)), None),
      //          (Key.SubMapsStart(Seq(1)), None),
      //          (Key.SubMapsEnd(Seq(1)), None),
      //          (Key.End(Seq(1)), None)
      //        )

      rootMap.closeDatabase().get
    }

    "update range keys" in {
      val rootMap = newDB()
      rootMap.put(1, "one").runIO
      rootMap.put(2, "two").runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO

      subMap.put(1, "subMap two").runIO
      subMap.put(2, "subMap two").runIO
      subMap.put(3, "subMap two").runIO
      subMap.put(4, "subMap two").runIO

      eitherOne(
        left = {
          rootMap.update(1, 2, "updated").runIO //update all key-values from rootMap
          subMap.update(2, 3, "updated").runIO //update some from subMap
        },
        right = {
          rootMap.update(1, "updated").runIO
          rootMap.update(2, "updated").runIO
          subMap.update(2, "updated").runIO
          subMap.update(3, "updated").runIO
        }
      )

      rootMap.get(1).runIO.value shouldBe "updated"
      rootMap.get(2).runIO.value shouldBe "updated"
      subMap.get(2).assertGet shouldBe "updated"
      subMap.get(3).assertGet shouldBe "updated"

      rootMap.closeDatabase().get
    }

    "batch put" in {
      val rootMap = newDB()
      rootMap.commitPrepared(
        Prepare.Put(1, "one"),
        Prepare.Put(2, "two")
      ).runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO
      subMap.commitPrepared(
        Prepare.Put(1, "one one"),
        Prepare.Put(2, "two two")
      ).runIO

      rootMap.get(1).runIO.value shouldBe "one"
      rootMap.get(2).runIO.value shouldBe "two"
      subMap.get(1).assertGet shouldBe "one one"
      subMap.get(2).assertGet shouldBe "two two"

      rootMap.closeDatabase().get
    }

    "batch update" in {
      val rootMap = newDB()
      rootMap.commitPrepared(
        Prepare.Put(1, "one"),
        Prepare.Put(2, "two")
      ).runIO

      rootMap.commitPrepared(
        Prepare.Update(1, "one updated"),
        Prepare.Update(2, "two updated")
      ).runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO
      subMap.commitPrepared(
        Prepare.Put(1, "one one"),
        Prepare.Put(2, "two two")
      ).runIO

      subMap.commitPrepared(
        Prepare.Update(1, "one one updated"),
        Prepare.Update(2, "two two updated")
      ).runIO

      rootMap.get(1).runIO.value shouldBe "one updated"
      rootMap.get(2).runIO.value shouldBe "two updated"
      subMap.get(1).assertGet shouldBe "one one updated"
      subMap.get(2).assertGet shouldBe "two two updated"

      rootMap.closeDatabase().get
    }

    "batch expire" in {
      val rootMap = newDB()
      rootMap.commitPrepared(
        Prepare.Put(1, "one"),
        Prepare.Put(2, "two")
      ).runIO

      rootMap.commitPrepared(
        Prepare.Expire(1, 100.millisecond),
        Prepare.Expire(2, 100.millisecond)
      ).runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO
      subMap.commitPrepared(
        Prepare.Put(1, "one one"),
        Prepare.Put(2, "two two")
      ).runIO

      subMap.commitPrepared(
        Prepare.Expire(1, 100.millisecond),
        Prepare.Expire(2, 100.millisecond)
      ).runIO

      eventual {
        rootMap.stream.materialize.runIO shouldBe empty
        subMap.stream.materialize.runIO shouldBe empty
      }

      rootMap.closeDatabase().get
    }

    "batchPut" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two")).runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO
      subMap.put((1, "one one"), (2, "two two"))

      rootMap.stream.materialize.runIO shouldBe ListBuffer((1, "one"), (2, "two"))
      subMap.stream.materialize.runIO shouldBe ListBuffer((1, "one one"), (2, "two two"))

      rootMap.closeDatabase().get
    }

    "batchUpdate" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two")).runIO
      rootMap.update((1, "one updated"), (2, "two updated")).runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO
      subMap.put((1, "one one"), (2, "two two"))
      subMap.update((1, "one one updated"), (2, "two two updated")).runIO

      rootMap.stream.materialize.runIO shouldBe ListBuffer((1, "one updated"), (2, "two updated"))
      subMap.stream.materialize.runIO shouldBe ListBuffer((1, "one one updated"), (2, "two two updated"))

      rootMap.closeDatabase().get
    }

    "batchRemove" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two")).runIO
      rootMap.remove(1, 2).runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO
      subMap.put((1, "one one"), (2, "two two"))
      subMap.remove(1, 2).runIO

      rootMap.stream.materialize.runIO shouldBe empty
      subMap.stream.materialize.runIO shouldBe empty

      rootMap.closeDatabase().get
    }

    "batchExpire" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two")).runIO
      rootMap.expire((1, 1.second.fromNow)).runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO
      subMap.put((1, "one one"), (2, "two two"))
      subMap.expire((1, 1.second.fromNow), (2, 1.second.fromNow)).runIO

      eventual {
        rootMap.stream.materialize.runIO should contain only ((2, "two"))
        subMap.stream.materialize.runIO shouldBe empty
      }

      rootMap.closeDatabase().get
    }

    "get" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two")).runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO
      subMap.put((1, "one one"), (2, "two two"))

      rootMap.get(1).runIO.value shouldBe "one"
      rootMap.get(2).runIO.value shouldBe "two"
      subMap.get(1).assertGet shouldBe "one one"
      subMap.get(2).assertGet shouldBe "two two"

      rootMap.remove(1, 2).runIO
      subMap.remove(1, 2).runIO

      rootMap.get(1).runIO shouldBe empty
      rootMap.get(2).runIO shouldBe empty
      subMap.get(1).assertGetOpt shouldBe empty
      subMap.get(2).assertGetOpt shouldBe empty

      rootMap.closeDatabase().get
    }

    "value when sub map is removed" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two")).runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO
      subMap.put((1, "one one"), (2, "two two"))

      rootMap.get(1).runIO.value shouldBe "one"
      rootMap.get(2).runIO.value shouldBe "two"
      subMap.get(1).assertGet shouldBe "one one"
      subMap.get(2).assertGet shouldBe "two two"

      rootMap.remove(1, 2).runIO
      rootMap.maps.remove(1).runIO

      rootMap.get(1).runIO shouldBe empty
      rootMap.get(2).runIO shouldBe empty
      subMap.get(1).assertGetOpt shouldBe empty
      subMap.get(2).assertGetOpt shouldBe empty

      rootMap.closeDatabase().get
    }

    "getKey" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two")).runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO
      subMap.put((11, "one one"), (22, "two two"))

      rootMap.getKey(1).runIO.value shouldBe 1
      rootMap.getKey(2).runIO.value shouldBe 2
      subMap.getKey(11).assertGet shouldBe 11
      subMap.getKey(22).assertGet shouldBe 22

      rootMap.remove(1, 2).runIO
      rootMap.maps.remove(1).runIO

      rootMap.get(1).runIO shouldBe empty
      rootMap.get(2).runIO shouldBe empty
      subMap.get(11).assertGetOpt shouldBe empty
      subMap.get(22).assertGetOpt shouldBe empty

      rootMap.closeDatabase().get
    }

    "getKeyValue" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two")).runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO
      subMap.put((11, "one one"), (22, "two two"))

      rootMap.getKeyValue(1).runIO.value shouldBe(1, "one")
      rootMap.getKeyValue(2).runIO.value shouldBe(2, "two")
      subMap.getKeyValue(11).assertGet shouldBe(11, "one one")
      subMap.getKeyValue(22).assertGet shouldBe(22, "two two")

      rootMap.remove(1, 2).runIO
      rootMap.maps.remove(1).runIO

      rootMap.getKeyValue(1).runIO shouldBe empty
      rootMap.getKeyValue(2).runIO shouldBe empty
      subMap.getKeyValue(11).assertGetOpt shouldBe empty
      subMap.getKeyValue(22).assertGetOpt shouldBe empty

      rootMap.closeDatabase().get
    }

    "keys" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two")).runIO

      val subMap = rootMap.maps.put(1, "sub map").runIO
      subMap.put((11, "one one"), (22, "two two"))

      rootMap.keys.stream.materialize.runIO should contain inOrderOnly(1, 2)
      subMap.keys.stream.materialize.runIO should contain inOrderOnly(11, 22)

      rootMap.closeDatabase().get
    }
  }

  "Map" should {

    "return entries ranges" in {
      Map.entriesRangeKeys(Seq(1, 2, 3)) shouldBe ((Key.MapEntriesStart(Seq(1, 2, 3)), Key.MapEntriesEnd(Seq(1, 2, 3))))
    }

    "return empty subMap range keys for a empty SubMap" in {
      val db = newDB()

      val rootMap = db.maps.put(1, "rootMap").runIO
      Map.childSubMapRanges(rootMap).get shouldBe empty

      db.closeDatabase().get
    }

    "return subMap that has only one child subMap" in {
      val rootMap = newDB()

      val firstMap = rootMap.maps.put(1, "rootMap").runIO
      val secondMap = firstMap.maps.put(2, "second map").runIO

      Map.childSubMapRanges(firstMap).get should contain only ((Key.SubMap(Seq(1), 2), Key.MapStart(Seq(1, 2)), Key.MapEnd(Seq(1, 2))))
      Map.childSubMapRanges(secondMap).get shouldBe empty

      rootMap.closeDatabase().get
    }

    "return subMaps of 3 nested maps" in {
      val db = newDB()

      val firstMap = db.maps.put(1, "first").runIO
      val secondMap = firstMap.maps.put(2, "second").runIO
      val thirdMap = secondMap.maps.put(2, "third").runIO

      Map.childSubMapRanges(firstMap).get should contain inOrderOnly((Key.SubMap(Seq(1), 2), Key.MapStart(Seq(1, 2)), Key.MapEnd(Seq(1, 2))), (Key.SubMap(Seq(1, 2), 2), Key.MapStart(Seq(1, 2, 2)), Key.MapEnd(Seq(1, 2, 2))))
      Map.childSubMapRanges(secondMap).get should contain only ((Key.SubMap(Seq(1, 2), 2), Key.MapStart(Seq(1, 2, 2)), Key.MapEnd(Seq(1, 2, 2))))
      Map.childSubMapRanges(thirdMap).get shouldBe empty

      db.closeDatabase().get
    }

    "returns multiple child subMap that also contains nested subMaps" in {
      val db = newDB()

      val firstMap = db.maps.put(1, "firstMap").runIO
      val secondMap = firstMap.maps.put(2, "subMap").runIO

      secondMap.maps.put(2, "subMap").runIO
      secondMap.maps.put(3, "subMap3").runIO
      val subMap4 = secondMap.maps.put(4, "subMap4").runIO
      subMap4.maps.put(44, "subMap44").runIO
      val subMap5 = secondMap.maps.put(5, "subMap5").runIO
      val subMap55 = subMap5.maps.put(55, "subMap55").runIO
      subMap55.maps.put(5555, "subMap55").runIO
      subMap55.maps.put(6666, "subMap55").runIO
      subMap5.maps.put(555, "subMap555").runIO

      val mapHierarchy =
        List(
          (Key.SubMap(Seq(1), 2), Key.MapStart(Seq(1, 2)), Key.MapEnd(Seq(1, 2))),
          (Key.SubMap(Seq(1, 2), 2), Key.MapStart(Seq(1, 2, 2)), Key.MapEnd(Seq(1, 2, 2))),
          (Key.SubMap(Seq(1, 2), 3), Key.MapStart(Seq(1, 2, 3)), Key.MapEnd(Seq(1, 2, 3))),
          (Key.SubMap(Seq(1, 2), 4), Key.MapStart(Seq(1, 2, 4)), Key.MapEnd(Seq(1, 2, 4))),
          (Key.SubMap(Seq(1, 2, 4), 44), Key.MapStart(Seq(1, 2, 4, 44)), Key.MapEnd(Seq(1, 2, 4, 44))),
          (Key.SubMap(Seq(1, 2), 5), Key.MapStart(Seq(1, 2, 5)), Key.MapEnd(Seq(1, 2, 5))),
          (Key.SubMap(Seq(1, 2, 5), 55), Key.MapStart(Seq(1, 2, 5, 55)), Key.MapEnd(Seq(1, 2, 5, 55))),
          (Key.SubMap(Seq(1, 2, 5, 55), 5555), Key.MapStart(Seq(1, 2, 5, 55, 5555)), Key.MapEnd(Seq(1, 2, 5, 55, 5555))),
          (Key.SubMap(Seq(1, 2, 5, 55), 6666), Key.MapStart(Seq(1, 2, 5, 55, 6666)), Key.MapEnd(Seq(1, 2, 5, 55, 6666))),
          (Key.SubMap(Seq(1, 2, 5), 555), Key.MapStart(Seq(1, 2, 5, 555)), Key.MapEnd(Seq(1, 2, 5, 555)))
        )

      Map.childSubMapRanges(firstMap).get shouldBe mapHierarchy
      Map.childSubMapRanges(secondMap).get shouldBe mapHierarchy.drop(1)

      db.closeDatabase().get
    }
  }

  "SubMap" when {
    "maps.put on a non existing map" should {
      "create a new subMap" in {
        val root = newDB()

        val first = root.maps.put(1, "first").runIO
        val second = first.maps.put(2, "second").runIO
        first.maps.get(2).assertGetOpt shouldBe defined
        second.maps.get(2).assertGetOpt shouldBe empty

        root.closeDatabase().get
      }
    }

    "maps.put on a existing map" should {
      "replace existing map" in {
        val root = newDB()

        val first = root.maps.put(1, "first").runIO
        val second = first.maps.put(2, "second").runIO
        val secondAgain = first.maps.put(2, "second again").runIO

        first.maps.get(2).assertGetOpt shouldBe defined
        first.maps.getValue(2).assertGet shouldBe "second again"
        second.getValue().assertGet shouldBe "second again"
        secondAgain.getValue().assertGet shouldBe "second again"

        root.closeDatabase().get
      }

      "replace existing map and all it's entries" in {
        val root = newDB()

        val first = root.maps.put(1, "first").runIO
        val second = first.maps.put(2, "second").runIO
        //write entries to second map
        second.put(1, "one").runIO
        second.put(2, "two").runIO
        second.put(3, "three").runIO
        //assert second map has these entries
        second.stream.materialize.runIO shouldBe List((1, "one"), (2, "two"), (3, "three"))

        val secondAgain = first.maps.put(2, "second again").runIO

        //map value value updated
        first.maps.get(2).assertGetOpt shouldBe defined
        first.maps.getValue(2).assertGet shouldBe "second again"
        second.getValue().assertGet shouldBe "second again"
        secondAgain.getValue().assertGet shouldBe "second again"
        //all the old entries are removed
        second.stream.materialize.runIO shouldBe empty

        root.closeDatabase().get
      }

      "replace existing map and all it's entries and also all existing maps subMap and all their entries" in {
        val root = newDB()

        //MAP HIERARCHY
        //first
        //   second
        //       third
        //           fourth
        val first = root.maps.put(1, "first").runIO
        val second = first.maps.put(2, "second").runIO
        second.put(1, "second one").runIO
        second.put(2, "second two").runIO
        second.put(3, "second three").runIO
        //third map that is the child map of second map
        val third = second.maps.put(3, "third").runIO
        third.put(1, "third one").runIO
        third.put(2, "third two").runIO
        third.put(3, "third three").runIO
        val fourth = third.maps.put(4, "fourth").runIO
        fourth.put(1, "fourth one").runIO
        fourth.put(2, "fourth two").runIO
        fourth.put(3, "fourth three").runIO

        /**
          * Assert that the all maps' content is accurate
          */
        second.stream.materialize.runIO shouldBe List((1, "second one"), (2, "second two"), (3, "second three"))
        third.stream.materialize.runIO shouldBe List((1, "third one"), (2, "third two"), (3, "third three"))
        fourth.stream.materialize.runIO shouldBe List((1, "fourth one"), (2, "fourth two"), (3, "fourth three"))

        second.stream.materialize.runIO shouldBe List((1, "second one"), (2, "second two"), (3, "second three"))
        third.stream.materialize.runIO shouldBe List((1, "third one"), (2, "third two"), (3, "third three"))
        fourth.stream.materialize.runIO shouldBe List((1, "fourth one"), (2, "fourth two"), (3, "fourth three"))

        second.maps.stream.materialize.runIO shouldBe List((3, "third"))
        third.maps.stream.materialize.runIO shouldBe List((4, "fourth"))
        fourth.maps.stream.materialize.runIO shouldBe empty

        //submit put on second map and assert that all it's contents are replaced.
        val secondAgain = first.maps.put(2, "second updated").runIO

        //map value value updated
        first.maps.get(2).assertGetOpt shouldBe defined
        first.maps.getValue(2).assertGet shouldBe "second updated"
        second.getValue().assertGet shouldBe "second updated"
        secondAgain.getValue().assertGet shouldBe "second updated"
        //all the old entries are removed
        second.stream.materialize.runIO shouldBe empty
        third.stream.materialize.runIO shouldBe empty
        fourth.stream.materialize.runIO shouldBe empty

        second.maps.contains(3).runIO shouldBe false
        second.maps.contains(4).runIO shouldBe false

        root.closeDatabase().get
      }
    }

    "clear" should {
      "remove all key-values from a map" in {

        val root = newDB()
        val first = root.maps.put(1, "first").runIO
        val second = first.maps.put(2, "second").runIO
        second.put(1, "second one").runIO
        second.put(2, "second two").runIO
        second.put(3, "second three").runIO
        //third map that is the child map of second map
        val third = second.maps.put(3, "third").runIO
        third.put(1, "third one").runIO
        third.put(2, "third two").runIO
        third.put(3, "third three").runIO

        second.stream.materialize.runIO should have size 3
        second.clear().runIO
        second.stream.materialize.runIO shouldBe empty

        third.stream.materialize.runIO should have size 3
        second.maps.clear(3).runIO
        third.stream.materialize.runIO shouldBe empty

        root.closeDatabase().get
      }
    }
  }
}
