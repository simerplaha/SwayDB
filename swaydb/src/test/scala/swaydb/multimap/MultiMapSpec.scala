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

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.{Bag, MultiMap, Prepare, MultiMapKey => Key}
import swaydb.api.TestBaseEmbedded
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import Key._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class MultiMapSpec0 extends MultiMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): MultiMap[Int, String, Nothing, Bag.Less] =
    swaydb.persistent.MultiMap[Int, String, Nothing, Bag.Less](dir = randomDir)
}

class MultiMapSpec1 extends MultiMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): MultiMap[Int, String, Nothing, Bag.Less] =
    swaydb.persistent.MultiMap[Int, String, Nothing, Bag.Less](dir = randomDir, mapSize = 1.byte)
}

class MultiMapSpec2 extends MultiMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): MultiMap[Int, String, Nothing, Bag.Less] =
    swaydb.memory.MultiMap[Int, String, Nothing, Bag.Less]()
}

class MultiMapSpec3 extends MultiMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): MultiMap[Int, String, Nothing, Bag.Less] =
    swaydb.memory.MultiMap[Int, String, Nothing, Bag.Less](mapSize = 1.byte)
}

sealed trait MultiMapSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): MultiMap[Int, String, Nothing, Bag.Less]

  implicit val bag = Bag.less

  //  implicit val mapKeySerializer = Key.serializer(IntSerializer)
  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  "MultiMap" should {
    "initialise a rootMap" in {
      val rootMap = newDB()

      rootMap.stream.materialize.toList shouldBe empty

      //assert
      rootMap.map.stream.materialize.toList shouldBe
        List(
          (Key.MapStart(Iterable.empty), None),
          (Key.MapEntriesStart(Iterable.empty), None),
          (Key.MapEntriesEnd(Iterable.empty), None),
          (Key.SubMapsStart(Iterable.empty), None),
          (Key.SubMapsEnd(Iterable.empty), None),
          (Key.MapEnd(Iterable.empty), None)
        )

      rootMap.delete()
    }

    "insert key-values to rootMap" in {
      val rootMap = newDB()
      rootMap.put(1, "one")
      rootMap.put(2, "two")

      rootMap.get(1).value shouldBe "one"
      rootMap.get(2).value shouldBe "two"

      rootMap.stream.materialize.toList should contain only((1, "one"), (2, "two"))

      //assert
      rootMap.map.stream.materialize.toList shouldBe
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

      rootMap.delete()
    }

    "insert a childMap" in {
      val rootMap = newDB()
      rootMap.put(1, "one")
      rootMap.put(2, "two")

      rootMap.children.get(1) shouldBe empty

      val childMap = rootMap.children.init(1)

      rootMap.children.get(1) shouldBe defined

      childMap.put(1, "childMap one")
      childMap.put(2, "childMap two")

      rootMap.stream.materialize shouldBe ListBuffer((1, "one"), (2, "two"))
      childMap.stream.materialize shouldBe ListBuffer((1, "childMap one"), (2, "childMap two"))

      //assert
      rootMap.map.stream.materialize.toList shouldBe
        List(
          (Key.MapStart(Seq.empty), None),
          (Key.MapEntriesStart(Seq.empty), None),
          (Key.MapEntry(Seq.empty, 1), Some("one")),
          (Key.MapEntry(Seq.empty, 2), Some("two")),
          (Key.MapEntriesEnd(Seq.empty), None),
          (Key.SubMapsStart(Seq.empty), None),
          (Key.SubMap(Seq.empty, 1), None),
          (Key.SubMapsEnd(Seq.empty), None),
          (Key.MapEnd(Seq.empty), None),

          //childMaps entries
          (Key.MapStart(Seq(1)), None),
          (Key.MapEntriesStart(Seq(1)), None),
          (Key.MapEntry(Seq(1), 1), Some("childMap one")),
          (Key.MapEntry(Seq(1), 2), Some("childMap two")),
          (Key.MapEntriesEnd(Seq(1)), None),
          (Key.SubMapsStart(Seq(1)), None),
          (Key.SubMapsEnd(Seq(1)), None),
          (Key.MapEnd(Seq(1)), None)
        )

      rootMap.delete()
    }

    "remove all entries from rootMap and childMap" in {
      runThisParallel(10.times) {
        val rootMap = newDB()
        rootMap.put(1, "one")
        rootMap.put(2, "two")

        val childMap = rootMap.children.init(1)

        childMap.put(1, "childMap one")
        childMap.put(2, "childMap two")

        eitherOne(
          left = {
            rootMap.clear()
            childMap.clear()
          },
          right = {
            rootMap.remove(1, 2)
            childMap.remove(1, 2)
          }
        )

        childMap.stream.materialize.toList shouldBe empty

        childMap.put(3, "three")
        childMap.stream.materialize.toList shouldBe List((3, "three"))

        //assert
        //      rootMap.map.stream.materialize.toList shouldBe
        //        List(
        //          (Key.Start(Seq.empty), None),
        //          (Key.EntriesStart(Seq.empty), None),
        //          (Key.EntriesEnd(Seq.empty), None),
        //          (Key.SubMapsStart(Seq.empty), None),
        //          (Key.SubMap(Seq.empty, 1), Some("sub map")),
        //          (Key.SubMapsEnd(Seq.empty), None),
        //          (Key.End(Seq.empty), None),
        //
        //          //childMaps entries
        //          (Key.Start(Seq(1)), Some("sub map")),
        //          (Key.EntriesStart(Seq(1)), None),
        //          (Key.EntriesEnd(Seq(1)), None),
        //          (Key.SubMapsStart(Seq(1)), None),
        //          (Key.SubMapsEnd(Seq(1)), None),
        //          (Key.End(Seq(1)), None)
        //        )

        rootMap.delete()
      }
    }

    "get & remove" in {
      val rootMap = newDB()

      val child1 = rootMap.children.init(1)
      child1.put(1, "one")
      child1.put(2, "two")

      val child2 = rootMap.children.init(2)

      val child1Get = rootMap.children.get(1).value
      eitherOne(
        child1Get.stream.materialize.toList shouldBe ListBuffer((1, "one"), (2, "two")),
        child1.stream.materialize.toList shouldBe ListBuffer((1, "one"), (2, "two"))
      )

      rootMap.children.keys.materialize.toList shouldBe List(1, 2)
      rootMap.children.remove(1)
      rootMap.children.keys.materialize.toList shouldBe List(2)
      rootMap.children.remove(2)
      rootMap.children.keys.materialize.toList shouldBe empty

      rootMap.delete()
    }

    "expire key" in {
      val rootMap = newDB()
      rootMap.put(1, "one", 500.millisecond)
      rootMap.put(2, "two")

      val childMap = rootMap.children.init(1)

      childMap.put(1, "childMap one", 500.millisecond)
      childMap.put(2, "childMap two")

      eventual {
        rootMap.get(1) shouldBe empty
        childMap.get(1) shouldBe empty
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
      //          //childMaps entries
      //          (Key.Start(Seq(1)), Some("sub map")),
      //          (Key.EntriesStart(Seq(1)), None),
      //          //          (Key.Entry(Seq(1), 1), Some("childMap one")), //expired
      //          (Key.Entry(Seq(1), 2), Some("childMap two")),
      //          (Key.EntriesEnd(Seq(1)), None),
      //          (Key.SubMapsStart(Seq(1)), None),
      //          (Key.SubMapsEnd(Seq(1)), None),
      //          (Key.End(Seq(1)), None)
      //        )

      rootMap.delete()
    }

    "expire range keys" in {
      val rootMap = newDB()
      rootMap.put(1, "one")
      rootMap.put(2, "two")

      val childMap = rootMap.children.init(1)

      childMap.put(1, "childMap two")
      childMap.put(2, "childMap two")
      childMap.put(3, "childMap two")
      childMap.put(4, "childMap two")

      rootMap.expire(1, 2, 100.millisecond) //expire all key-values from rootMap
      childMap.expire(2, 3, 100.millisecond) //expire some from childMap

      eventual {
        rootMap.get(1) shouldBe empty
        rootMap.get(2) shouldBe empty
        childMap.get(1).value shouldBe "childMap two"
        childMap.get(2) shouldBe empty
        childMap.get(3) shouldBe empty
        childMap.get(4).value shouldBe "childMap two"
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
      //          //childMaps entries
      //          (Key.Start(Seq(1)), Some("sub map")),
      //          (Key.EntriesStart(Seq(1)), None),
      //          (Key.Entry(Seq(1), 1), Some("childMap two")),
      //          (Key.Entry(Seq(1), 4), Some("childMap two")),
      //          (Key.EntriesEnd(Seq(1)), None),
      //          (Key.SubMapsStart(Seq(1)), None),
      //          (Key.SubMapsEnd(Seq(1)), None),
      //          (Key.End(Seq(1)), None)
      //        )

      rootMap.delete()
    }

    "update range keys" in {
      val rootMap = newDB()
      rootMap.put(1, "one")
      rootMap.put(2, "two")

      val childMap = rootMap.children.init(1)

      childMap.put(1, "childMap two")
      childMap.put(2, "childMap two")
      childMap.put(3, "childMap two")
      childMap.put(4, "childMap two")

      eitherOne(
        left = {
          rootMap.update(1, 2, "updated") //update all key-values from rootMap
          childMap.update(2, 3, "updated") //update some from childMap
        },
        right = {
          rootMap.update(1, "updated")
          rootMap.update(2, "updated")
          childMap.update(2, "updated")
          childMap.update(3, "updated")
        }
      )

      rootMap.get(1).value shouldBe "updated"
      rootMap.get(2).value shouldBe "updated"
      childMap.get(2).value shouldBe "updated"
      childMap.get(3).value shouldBe "updated"

      rootMap.delete()
    }

    "batch put" in {
      val rootMap = newDB()
      rootMap.commit(
        Prepare.Put(1, "one"),
        Prepare.Put(2, "two")
      )

      val childMap = rootMap.children.init(1)
      childMap.commit(
        Prepare.Put(1, "one one"),
        Prepare.Put(2, "two two")
      )

      rootMap.get(1).value shouldBe "one"
      rootMap.get(2).value shouldBe "two"
      childMap.get(1).value shouldBe "one one"
      childMap.get(2).value shouldBe "two two"

      rootMap.delete()
    }

    "batch update" in {
      val rootMap = newDB()
      rootMap.commit(
        Prepare.Put(1, "one"),
        Prepare.Put(2, "two")
      )

      rootMap.commit(
        Prepare.Update(1, "one updated"),
        Prepare.Update(2, "two updated")
      )

      val childMap = rootMap.children.init(1)
      childMap.commit(
        Prepare.Put(1, "one one"),
        Prepare.Put(2, "two two")
      )

      childMap.commit(
        Prepare.Update(1, "one one updated"),
        Prepare.Update(2, "two two updated")
      )

      rootMap.get(1).value shouldBe "one updated"
      rootMap.get(2).value shouldBe "two updated"
      childMap.get(1).value shouldBe "one one updated"
      childMap.get(2).value shouldBe "two two updated"

      rootMap.delete()
    }

    "batch expire" in {
      val rootMap = newDB()
      rootMap.commit(
        Prepare.Put(1, "one"),
        Prepare.Put(2, "two")
      )

      rootMap.commit(
        Prepare.Expire(1, 100.millisecond),
        Prepare.Expire(2, 100.millisecond)
      )

      val childMap = rootMap.children.init(1)
      childMap.commit(
        Prepare.Put(1, "one one"),
        Prepare.Put(2, "two two")
      )

      childMap.commit(
        Prepare.Expire(1, 100.millisecond),
        Prepare.Expire(2, 100.millisecond)
      )

      eventual {
        rootMap.stream.materialize shouldBe empty
        childMap.stream.materialize shouldBe empty
      }

      rootMap.delete()
    }

    "batchPut" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two"))

      val childMap = rootMap.children.init(1)
      childMap.put((1, "one one"), (2, "two two"))

      rootMap.stream.materialize shouldBe ListBuffer((1, "one"), (2, "two"))
      childMap.stream.materialize shouldBe ListBuffer((1, "one one"), (2, "two two"))

      rootMap.delete()
    }

    "batchUpdate" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two"))
      rootMap.update((1, "one updated"), (2, "two updated"))

      val childMap = rootMap.children.init(1)
      childMap.put((1, "one one"), (2, "two two"))
      childMap.update((1, "one one updated"), (2, "two two updated"))

      rootMap.stream.materialize shouldBe ListBuffer((1, "one updated"), (2, "two updated"))
      childMap.stream.materialize shouldBe ListBuffer((1, "one one updated"), (2, "two two updated"))

      rootMap.delete()
    }

    "batchRemove" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two"))
      rootMap.remove(1, 2)

      val childMap = rootMap.children.init(1)
      childMap.put((1, "one one"), (2, "two two"))
      childMap.remove(1, 2)

      rootMap.stream.materialize shouldBe empty
      childMap.stream.materialize shouldBe empty

      rootMap.delete()
    }

    "batchExpire" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two"))
      rootMap.expire((1, 1.second.fromNow))

      val childMap = rootMap.children.init(1)
      childMap.put((1, "one one"), (2, "two two"))
      childMap.expire((1, 1.second.fromNow), (2, 1.second.fromNow))

      eventual {
        rootMap.stream.materialize.toList should contain only ((2, "two"))
        childMap.stream.materialize shouldBe empty
      }

      rootMap.delete()
    }

    "get" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two"))

      val childMap = rootMap.children.init(1)
      childMap.put((1, "one one"), (2, "two two"))

      rootMap.get(1).value shouldBe "one"
      rootMap.get(2).value shouldBe "two"
      childMap.get(1).value shouldBe "one one"
      childMap.get(2).value shouldBe "two two"

      rootMap.remove(1, 2)
      childMap.remove(1, 2)

      rootMap.get(1) shouldBe empty
      rootMap.get(2) shouldBe empty
      childMap.get(1) shouldBe empty
      childMap.get(2) shouldBe empty

      rootMap.delete()
    }

    "value when sub map is removed" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two"))

      val childMap = rootMap.children.init(1)
      childMap.put((1, "one one"), (2, "two two"))

      rootMap.get(1).value shouldBe "one"
      rootMap.get(2).value shouldBe "two"
      childMap.get(1).value shouldBe "one one"
      childMap.get(2).value shouldBe "two two"

      rootMap.remove(1, 2)
      rootMap.children.remove(1)

      rootMap.get(1) shouldBe empty
      rootMap.get(2) shouldBe empty
      childMap.get(1) shouldBe empty
      childMap.get(2) shouldBe empty

      rootMap.delete()
    }

    "getKey" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two"))

      val childMap = rootMap.children.init(1)
      childMap.put((11, "one one"), (22, "two two"))

      rootMap.getKey(1).value shouldBe 1
      rootMap.getKey(2).value shouldBe 2
      childMap.getKey(11).value shouldBe 11
      childMap.getKey(22).value shouldBe 22

      rootMap.remove(1, 2)
      rootMap.children.remove(1)

      rootMap.get(1) shouldBe empty
      rootMap.get(2) shouldBe empty
      childMap.get(11) shouldBe empty
      childMap.get(22) shouldBe empty

      rootMap.delete()
    }

    "getKeyValue" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two"))

      val childMap = rootMap.children.init(1)
      childMap.put((11, "one one"), (22, "two two"))

      rootMap.getKeyValue(1).value shouldBe(1, "one")
      rootMap.getKeyValue(2).value shouldBe(2, "two")
      childMap.getKeyValue(11).value shouldBe(11, "one one")
      childMap.getKeyValue(22).value shouldBe(22, "two two")

      rootMap.remove(1, 2)
      rootMap.children.remove(1)

      rootMap.getKeyValue(1) shouldBe empty
      rootMap.getKeyValue(2) shouldBe empty
      childMap.getKeyValue(11) shouldBe empty
      childMap.getKeyValue(22) shouldBe empty

      rootMap.delete()
    }

    "keys" in {
      val rootMap = newDB()
      rootMap.put((1, "one"), (2, "two"))

      val childMap = rootMap.children.init(1)
      childMap.put((11, "one one"), (22, "two two"))

      rootMap.stream.materialize.toList.map(_._1) should contain inOrderOnly(1, 2)
      childMap.stream.materialize.toList.map(_._1) should contain inOrderOnly(11, 22)

      rootMap.delete()
    }
  }

  //  "Map" should {
  //
  //    "return entries ranges" in {
  //      Map.entriesRangeKeys(Seq(1, 2, 3)) shouldBe ((Key.MapEntriesStart(Seq(1, 2, 3)), Key.MapEntriesEnd(Seq(1, 2, 3))))
  //    }
  //
  //    "return empty childMap range keys for a empty SubMap" in {
  //      val db = newDB()
  //
  //      val rootMap = db.children.init(1, "rootMap")
  //      Map.childSubMapRanges(rootMap).get shouldBe empty
  //
  //      db.delete()
  //    }
  //
  //    "return childMap that has only one child childMap" in {
  //      val rootMap = newDB()
  //
  //      val firstMap = rootMap.children.init(1, "rootMap")
  //      val secondMap = firstMap.children.init(2, "second map")
  //
  //      Map.childSubMapRanges(firstMap).get should contain only ((Key.SubMap(Seq(1), 2), Key.MapStart(Seq(1, 2)), Key.MapEnd(Seq(1, 2))))
  //      Map.childSubMapRanges(secondMap).get shouldBe empty
  //
  //      rootMap.delete()
  //    }
  //
  //    "return childMaps of 3 nested maps" in {
  //      val db = newDB()
  //
  //      val firstMap = db.children.init(1, "first")
  //      val secondMap = firstMap.children.init(2, "second")
  //      val thirdMap = secondMap.children.init(2, "third")
  //
  //      Map.childSubMapRanges(firstMap).get should contain inOrderOnly((Key.SubMap(Seq(1), 2), Key.MapStart(Seq(1, 2)), Key.MapEnd(Seq(1, 2))), (Key.SubMap(Seq(1, 2), 2), Key.MapStart(Seq(1, 2, 2)), Key.MapEnd(Seq(1, 2, 2))))
  //      Map.childSubMapRanges(secondMap).get should contain only ((Key.SubMap(Seq(1, 2), 2), Key.MapStart(Seq(1, 2, 2)), Key.MapEnd(Seq(1, 2, 2))))
  //      Map.childSubMapRanges(thirdMap).get shouldBe empty
  //
  //      db.delete()
  //    }
  //
  //    "returns multiple child childMap that also contains nested childMaps" in {
  //      val db = newDB()
  //
  //      val firstMap = db.children.init(1, "firstMap")
  //      val secondMap = firstMap.children.init(2, "childMap")
  //
  //      secondMap.children.init(2, "childMap")
  //      secondMap.children.init(3, "childMap3")
  //      val childMap4 = secondMap.children.init(4, "childMap4")
  //      childMap4.children.init(44, "childMap44")
  //      val childMap5 = secondMap.children.init(5, "childMap5")
  //      val childMap55 = childMap5.children.init(55, "childMap55")
  //      childMap55.children.init(5555, "childMap55")
  //      childMap55.children.init(6666, "childMap55")
  //      childMap5.children.init(555, "childMap555")
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
  //      Map.childSubMapRanges(firstMap).get shouldBe mapHierarchy
  //      Map.childSubMapRanges(secondMap).get shouldBe mapHierarchy.drop(1)
  //
  //      db.delete()
  //    }
  //  }
  //
  "SubMap" when {
    "children.init on a non existing map" should {
      "create a new childMap" in {
        val root = newDB()

        val first = root.children.init(1)
        val second = first.children.init(2)
        first.children.get(2) shouldBe defined
        second.children.get(2) shouldBe empty

        root.delete()
      }
    }

    "children.init on a existing map" should {
      "replace existing map" in {
        val root = newDB()

        val first = root.children.init(1)
        val second = first.children.init(2)
        val secondAgain = first.children.replace(2)

        first.children.get(2) shouldBe defined

        root.delete()
      }

      "replace existing map and all it's entries" in {
        val root = newDB()

        val first = root.children.init(1)
        val second = first.children.init(2)
        //write entries to second map
        second.put(1, "one")
        second.put(2, "two")
        second.put(3, "three")
        //assert second map has these entries
        second.stream.materialize shouldBe List((1, "one"), (2, "two"), (3, "three"))

        val secondAgain = first.children.replace(2)

        //map value value updated
        first.children.get(2) shouldBe defined
        //all the old entries are removed
        second.stream.materialize shouldBe empty

        root.delete()
      }

      "replace existing map and all it's entries and also all existing maps childMap and all their entries" in {
        val root = newDB()

        //MAP HIERARCHY
        //first
        //   second
        //       third
        //           fourth
        val first = root.children.init(1)
        val second = first.children.init(2)
        second.put(1, "second one")
        second.put(2, "second two")
        second.put(3, "second three")
        //third map that is the child map of second map
        val third = second.children.init(3)
        third.put(1, "third one")
        third.put(2, "third two")
        third.put(3, "third three")
        val fourth = third.children.init(4)
        fourth.put(1, "fourth one")
        fourth.put(2, "fourth two")
        fourth.put(3, "fourth three")

        /**
         * Assert that the all maps' content is accurate
         */
        second.stream.materialize shouldBe List((1, "second one"), (2, "second two"), (3, "second three"))
        third.stream.materialize shouldBe List((1, "third one"), (2, "third two"), (3, "third three"))
        fourth.stream.materialize shouldBe List((1, "fourth one"), (2, "fourth two"), (3, "fourth three"))

        second.stream.materialize shouldBe List((1, "second one"), (2, "second two"), (3, "second three"))
        third.stream.materialize shouldBe List((1, "third one"), (2, "third two"), (3, "third three"))
        fourth.stream.materialize shouldBe List((1, "fourth one"), (2, "fourth two"), (3, "fourth three"))

        second.children.keys.materialize.toList should contain only 3
        third.children.keys.materialize.toList should contain only 4
        fourth.children.keys.materialize.toList shouldBe empty

        //submit put on second map and assert that all it's contents are replaced.
        val secondAgain = first.children.replace(2)

        //map value value updated
        first.children.get(2) shouldBe defined
        //all the old entries are removed
        second.stream.materialize shouldBe empty
        third.stream.materialize shouldBe empty
        fourth.stream.materialize shouldBe empty

        second.children.get(3) shouldBe empty
        second.children.get(4) shouldBe empty

        root.delete()
      }
    }

    //    "clear" should {
    //      "remove all key-values from a map" in {
    //
    //        val root = newDB()
    //        val first = root.children.init(1, "first")
    //        val second = first.children.init(2, "second")
    //        second.put(1, "second one")
    //        second.put(2, "second two")
    //        second.put(3, "second three")
    //        //third map that is the child map of second map
    //        val third = second.children.init(3, "third")
    //        third.put(1, "third one")
    //        third.put(2, "third two")
    //        third.put(3, "third three")
    //
    //        second.stream.materialize should have size 3
    //        second.clear()
    //        second.stream.materialize shouldBe empty
    //
    //        third.stream.materialize should have size 3
    //        second.maps.clear(3)
    //        third.stream.materialize shouldBe empty
    //
    //        root.delete()
    //      }
    //    }
  }
}
