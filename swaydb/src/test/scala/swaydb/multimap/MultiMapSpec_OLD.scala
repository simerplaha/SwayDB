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
import swaydb.api.TestBaseEmbedded
import swaydb.core.CommonAssertions._
import swaydb.data.RunThis._
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.{Bag, MultiMap_EAP, MultiMapKey, Prepare}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class MultiMapSpecOLD0 extends MultiMapSpec_OLD {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): MultiMap_EAP[Int, Int, String, Nothing, Bag.Less] =
    swaydb.persistent.MultiMap_EAP[Int, Int, String, Nothing, Bag.Less](dir = randomDir).sweep()
}

class MultiMapSpecOLD1 extends MultiMapSpec_OLD {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): MultiMap_EAP[Int, Int, String, Nothing, Bag.Less] =
    swaydb.persistent.MultiMap_EAP[Int, Int, String, Nothing, Bag.Less](dir = randomDir, mapSize = 1.byte).sweep()
}

class MultiMapSpecOLD2 extends MultiMapSpec_OLD {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): MultiMap_EAP[Int, Int, String, Nothing, Bag.Less] =
    swaydb.memory.MultiMap_EAP[Int, Int, String, Nothing, Bag.Less]().sweep()
}

class MultiMapSpecOLD3 extends MultiMapSpec_OLD {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): MultiMap_EAP[Int, Int, String, Nothing, Bag.Less] =
    swaydb.memory.MultiMap_EAP[Int, Int, String, Nothing, Bag.Less](mapSize = 1.byte).sweep()
}

/**
 * OLD test-cases when [[MultiMap_EAP]] was called Extension. Keeping these test around
 * because cover some use-cases.
 */
sealed trait MultiMapSpec_OLD extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB()(implicit sweeper: TestCaseSweeper): MultiMap_EAP[Int, Int, String, Nothing, Bag.Less]

  implicit val bag = Bag.less

  //  implicit val mapKeySerializer = MultiMapKey.serializer(IntSerializer)
  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default


  "initialising" should {
    "create an empty rootMap" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()

          rootMap.stream.materialize.toList shouldBe empty

          //assert
          rootMap.innerMap.stream.materialize.toList shouldBe
            List(
              (MultiMapKey.MapStart(Iterable.empty), None),
              (MultiMapKey.MapEntriesStart(Iterable.empty), None),
              (MultiMapKey.MapEntriesEnd(Iterable.empty), None),
              (MultiMapKey.SubMapsStart(Iterable.empty), None),
              (MultiMapKey.SubMapsEnd(Iterable.empty), None),
              (MultiMapKey.MapEnd(Iterable.empty), None)
            )
      }
    }

    "create a non-empty rootMap" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.put(1, "one")
          rootMap.put(2, "two")

          rootMap.get(1).value shouldBe "one"
          rootMap.get(2).value shouldBe "two"

          rootMap.stream.materialize.toList should contain only((1, "one"), (2, "two"))

          //assert
          rootMap.innerMap.stream.materialize.toList shouldBe
            List(
              (MultiMapKey.MapStart(Seq.empty), None),
              (MultiMapKey.MapEntriesStart(Seq.empty), None),
              (MultiMapKey.MapEntry(Seq.empty, 1), Some("one")),
              (MultiMapKey.MapEntry(Seq.empty, 2), Some("two")),
              (MultiMapKey.MapEntriesEnd(Seq.empty), None),
              (MultiMapKey.SubMapsStart(Seq.empty), None),
              (MultiMapKey.SubMapsEnd(Seq.empty), None),
              (MultiMapKey.MapEnd(Seq.empty), None)
            )
      }
    }

    "create with childMap" in {
      runThis(times = 10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val rootMap = newDB()
            rootMap.put(1, "one")
            rootMap.put(2, "two")

            rootMap.schema.get(1) shouldBe empty

            val childMap = rootMap.schema.init(1)

            rootMap.schema.get(1) shouldBe defined

            childMap.put(1, "childMap one")
            childMap.put(2, "childMap two")

            rootMap.stream.materialize shouldBe ListBuffer((1, "one"), (2, "two"))
            childMap.stream.materialize shouldBe ListBuffer((1, "childMap one"), (2, "childMap two"))

            //assert
            rootMap.innerMap.stream.materialize.toList shouldBe
              List(
                (MultiMapKey.MapStart(Seq.empty), None),
                (MultiMapKey.MapEntriesStart(Seq.empty), None),
                (MultiMapKey.MapEntry(Seq.empty, 1), Some("one")),
                (MultiMapKey.MapEntry(Seq.empty, 2), Some("two")),
                (MultiMapKey.MapEntriesEnd(Seq.empty), None),
                (MultiMapKey.SubMapsStart(Seq.empty), None),
                (MultiMapKey.SubMap(Seq.empty, 1), None),
                (MultiMapKey.SubMapsEnd(Seq.empty), None),
                (MultiMapKey.MapEnd(Seq.empty), None),

                //childMaps entries
                (MultiMapKey.MapStart(Seq(1)), None),
                (MultiMapKey.MapEntriesStart(Seq(1)), None),
                (MultiMapKey.MapEntry(Seq(1), 1), Some("childMap one")),
                (MultiMapKey.MapEntry(Seq(1), 2), Some("childMap two")),
                (MultiMapKey.MapEntriesEnd(Seq(1)), None),
                (MultiMapKey.SubMapsStart(Seq(1)), None),
                (MultiMapKey.SubMapsEnd(Seq(1)), None),
                (MultiMapKey.MapEnd(Seq(1)), None)
              )
        }
      }
    }
  }

  "childMap" should {
    "remove all entries" in {
      runThisParallel(10.times) {
        TestCaseSweeper {
          implicit sweeper =>

            val rootMap = newDB()
            rootMap.put(1, "one")
            rootMap.put(2, "two")

            val childMap = rootMap.schema.init(1)

            childMap.put(1, "childMap one")
            childMap.put(2, "childMap two")

            eitherOne(
              left = {
                rootMap.clearKeyValues()
                childMap.clearKeyValues()
              },
              right = {
                rootMap.remove(1, 2)
                childMap.remove(1, 2)
              }
            )

            childMap.stream.materialize.toList shouldBe empty

            childMap.put(3, "three")
            childMap.stream.materialize.toList shouldBe List((3, "three"))
        }
      }
    }

    "remove" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()

          val child1 = rootMap.schema.init(1)
          child1.put(1, "one")
          child1.put(2, "two")

          val child2 = rootMap.schema.init(2)

          val child1Get = rootMap.schema.get(1).value
          eitherOne(
            child1Get.stream.materialize.toList shouldBe ListBuffer((1, "one"), (2, "two")),
            child1.stream.materialize.toList shouldBe ListBuffer((1, "one"), (2, "two"))
          )

          rootMap.schema.keys.materialize.toList shouldBe List(1, 2)
          rootMap.schema.remove(1)
          rootMap.schema.keys.materialize.toList shouldBe List(2)
          rootMap.schema.remove(2)
          rootMap.schema.keys.materialize.toList shouldBe empty
      }
    }


    "expire key" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.put(1, "one", 500.millisecond)
          rootMap.put(2, "two")

          val childMap = rootMap.schema.init(1)

          childMap.put(1, "childMap one", 500.millisecond)
          childMap.put(2, "childMap two")

          eventual {
            rootMap.get(1) shouldBe empty
            childMap.get(1) shouldBe empty
          }

        //assert
        //      rootMap.baseMap().toList shouldBe
        //        List(
        //          (MultiMapKey.Start(Seq.empty), None),
        //          (MultiMapKey.EntriesStart(Seq.empty), None),
        //          //          (MultiMapKey.Entry(Seq.empty, 1), Some("one")),//expired
        //          (MultiMapKey.Entry(Seq.empty, 2), Some("two")),
        //          (MultiMapKey.EntriesEnd(Seq.empty), None),
        //          (MultiMapKey.SubMapsStart(Seq.empty), None),
        //          (MultiMapKey.SubMap(Seq.empty, 1), Some("sub map")),
        //          (MultiMapKey.SubMapsEnd(Seq.empty), None),
        //          (MultiMapKey.End(Seq.empty), None),
        //
        //          //childMaps entries
        //          (MultiMapKey.Start(Seq(1)), Some("sub map")),
        //          (MultiMapKey.EntriesStart(Seq(1)), None),
        //          //          (MultiMapKey.Entry(Seq(1), 1), Some("childMap one")), //expired
        //          (MultiMapKey.Entry(Seq(1), 2), Some("childMap two")),
        //          (MultiMapKey.EntriesEnd(Seq(1)), None),
        //          (MultiMapKey.SubMapsStart(Seq(1)), None),
        //          (MultiMapKey.SubMapsEnd(Seq(1)), None),
        //          (MultiMapKey.End(Seq(1)), None)
        //        )
      }
    }

    "expire range keys" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.put(1, "one")
          rootMap.put(2, "two")

          val childMap = rootMap.schema.init(1)

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
        //          (MultiMapKey.Start(Seq.empty), None),
        //          (MultiMapKey.EntriesStart(Seq.empty), None),
        //          (MultiMapKey.EntriesEnd(Seq.empty), None),
        //          (MultiMapKey.SubMapsStart(Seq.empty), None),
        //          (MultiMapKey.SubMap(Seq.empty, 1), Some("sub map")),
        //          (MultiMapKey.SubMapsEnd(Seq.empty), None),
        //          (MultiMapKey.End(Seq.empty), None),
        //
        //          //childMaps entries
        //          (MultiMapKey.Start(Seq(1)), Some("sub map")),
        //          (MultiMapKey.EntriesStart(Seq(1)), None),
        //          (MultiMapKey.Entry(Seq(1), 1), Some("childMap two")),
        //          (MultiMapKey.Entry(Seq(1), 4), Some("childMap two")),
        //          (MultiMapKey.EntriesEnd(Seq(1)), None),
        //          (MultiMapKey.SubMapsStart(Seq(1)), None),
        //          (MultiMapKey.SubMapsEnd(Seq(1)), None),
        //          (MultiMapKey.End(Seq(1)), None)
        //        )
      }
    }

    "update range keys" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.put(1, "one")
          rootMap.put(2, "two")

          val childMap = rootMap.schema.init(1)

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
      }
    }

    "batch put" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.commit(
            Prepare.Put(1, "one"),
            Prepare.Put(2, "two")
          )

          val childMap = rootMap.schema.init(1)
          childMap.commit(
            Prepare.Put(1, "one one"),
            Prepare.Put(2, "two two")
          )

          rootMap.get(1).value shouldBe "one"
          rootMap.get(2).value shouldBe "two"
          childMap.get(1).value shouldBe "one one"
          childMap.get(2).value shouldBe "two two"
      }
    }

    "batch update" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.commit(
            Prepare.Put(1, "one"),
            Prepare.Put(2, "two")
          )

          rootMap.commit(
            Prepare.Update(1, "one updated"),
            Prepare.Update(2, "two updated")
          )

          val childMap = rootMap.schema.init(1)
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
      }
    }

    "batch expire" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.commit(
            Prepare.Put(1, "one"),
            Prepare.Put(2, "two")
          )

          rootMap.commit(
            Prepare.Expire(1, 100.millisecond),
            Prepare.Expire(2, 100.millisecond)
          )

          val childMap = rootMap.schema.init(1)
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
      }
    }

    "batchPut" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.put((1, "one"), (2, "two"))

          val childMap = rootMap.schema.init(1)
          childMap.put((1, "one one"), (2, "two two"))

          rootMap.stream.materialize shouldBe ListBuffer((1, "one"), (2, "two"))
          childMap.stream.materialize shouldBe ListBuffer((1, "one one"), (2, "two two"))
      }
    }

    "batchUpdate" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.put((1, "one"), (2, "two"))
          rootMap.update((1, "one updated"), (2, "two updated"))

          val childMap = rootMap.schema.init(1)
          childMap.put((1, "one one"), (2, "two two"))
          childMap.update((1, "one one updated"), (2, "two two updated"))

          rootMap.stream.materialize shouldBe ListBuffer((1, "one updated"), (2, "two updated"))
          childMap.stream.materialize shouldBe ListBuffer((1, "one one updated"), (2, "two two updated"))
      }
    }

    "batchRemove" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.put((1, "one"), (2, "two"))
          rootMap.remove(1, 2)

          val childMap = rootMap.schema.init(1)
          childMap.put((1, "one one"), (2, "two two"))
          childMap.remove(1, 2)

          rootMap.stream.materialize shouldBe empty
          childMap.stream.materialize shouldBe empty
      }
    }

    "batchExpire" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.put((1, "one"), (2, "two"))
          rootMap.expire((1, 1.second.fromNow))

          val childMap = rootMap.schema.init(1)
          childMap.put((1, "one one"), (2, "two two"))
          childMap.expire((1, 1.second.fromNow), (2, 1.second.fromNow))

          eventual {
            rootMap.stream.materialize.toList should contain only ((2, "two"))
            childMap.stream.materialize shouldBe empty
          }
      }
    }

    "get" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.put((1, "one"), (2, "two"))

          val childMap = rootMap.schema.init(1)
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
      }
    }

    "value when sub map is removed" in {
      TestCaseSweeper {
        implicit sweeper =>
          val rootMap = newDB()
          rootMap.put((1, "one"), (2, "two"))

          val childMap = rootMap.schema.init(1)
          childMap.put((1, "one one"), (2, "two two"))

          rootMap.get(1).value shouldBe "one"
          rootMap.get(2).value shouldBe "two"
          childMap.get(1).value shouldBe "one one"
          childMap.get(2).value shouldBe "two two"

          rootMap.remove(1, 2)
          rootMap.schema.remove(1)

          rootMap.get(1) shouldBe empty
          rootMap.get(2) shouldBe empty
          childMap.get(1) shouldBe empty
          childMap.get(2) shouldBe empty
      }
    }

    "getKey" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.put((1, "one"), (2, "two"))

          val childMap = rootMap.schema.init(1)
          childMap.put((11, "one one"), (22, "two two"))

          rootMap.getKey(1).value shouldBe 1
          rootMap.getKey(2).value shouldBe 2
          childMap.getKey(11).value shouldBe 11
          childMap.getKey(22).value shouldBe 22

          rootMap.remove(1, 2)
          rootMap.schema.remove(1)

          rootMap.get(1) shouldBe empty
          rootMap.get(2) shouldBe empty
          childMap.get(11) shouldBe empty
          childMap.get(22) shouldBe empty
      }
    }

    "getKeyValue" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.put((1, "one"), (2, "two"))

          val childMap = rootMap.schema.init(1)
          childMap.put((11, "one one"), (22, "two two"))

          rootMap.getKeyValue(1).value shouldBe(1, "one")
          rootMap.getKeyValue(2).value shouldBe(2, "two")
          childMap.getKeyValue(11).value shouldBe(11, "one one")
          childMap.getKeyValue(22).value shouldBe(22, "two two")

          rootMap.remove(1, 2)
          rootMap.schema.remove(1)

          rootMap.getKeyValue(1) shouldBe empty
          rootMap.getKeyValue(2) shouldBe empty
          childMap.getKeyValue(11) shouldBe empty
          childMap.getKeyValue(22) shouldBe empty
      }
    }

    "keys" in {
      TestCaseSweeper {
        implicit sweeper =>

          val rootMap = newDB()
          rootMap.put((1, "one"), (2, "two"))

          val childMap = rootMap.schema.init(1)
          childMap.put((11, "one one"), (22, "two two"))

          rootMap.stream.materialize.toList.map(_._1) should contain inOrderOnly(1, 2)
          childMap.stream.materialize.toList.map(_._1) should contain inOrderOnly(11, 22)
      }
    }
  }


  //  "Map" should {
  //
  //    "return entries ranges" in {
  //      Map.entriesRangeKeys(Seq(1, 2, 3)) shouldBe ((MultiMapKey.MapEntriesStart(Seq(1, 2, 3)), MultiMapKey.MapEntriesEnd(Seq(1, 2, 3))))
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
  //      Map.childSubMapRanges(firstMap).get should contain only ((MultiMapKey.SubMap(Seq(1), 2), MultiMapKey.MapStart(Seq(1, 2)), MultiMapKey.MapEnd(Seq(1, 2))))
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
  //      Map.childSubMapRanges(firstMap).get should contain inOrderOnly((MultiMapKey.SubMap(Seq(1), 2), MultiMapKey.MapStart(Seq(1, 2)), MultiMapKey.MapEnd(Seq(1, 2))), (MultiMapKey.SubMap(Seq(1, 2), 2), MultiMapKey.MapStart(Seq(1, 2, 2)), MultiMapKey.MapEnd(Seq(1, 2, 2))))
  //      Map.childSubMapRanges(secondMap).get should contain only ((MultiMapKey.SubMap(Seq(1, 2), 2), MultiMapKey.MapStart(Seq(1, 2, 2)), MultiMapKey.MapEnd(Seq(1, 2, 2))))
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
  //          (MultiMapKey.SubMap(Seq(1), 2), MultiMapKey.MapStart(Seq(1, 2)), MultiMapKey.MapEnd(Seq(1, 2))),
  //          (MultiMapKey.SubMap(Seq(1, 2), 2), MultiMapKey.MapStart(Seq(1, 2, 2)), MultiMapKey.MapEnd(Seq(1, 2, 2))),
  //          (MultiMapKey.SubMap(Seq(1, 2), 3), MultiMapKey.MapStart(Seq(1, 2, 3)), MultiMapKey.MapEnd(Seq(1, 2, 3))),
  //          (MultiMapKey.SubMap(Seq(1, 2), 4), MultiMapKey.MapStart(Seq(1, 2, 4)), MultiMapKey.MapEnd(Seq(1, 2, 4))),
  //          (MultiMapKey.SubMap(Seq(1, 2, 4), 44), MultiMapKey.MapStart(Seq(1, 2, 4, 44)), MultiMapKey.MapEnd(Seq(1, 2, 4, 44))),
  //          (MultiMapKey.SubMap(Seq(1, 2), 5), MultiMapKey.MapStart(Seq(1, 2, 5)), MultiMapKey.MapEnd(Seq(1, 2, 5))),
  //          (MultiMapKey.SubMap(Seq(1, 2, 5), 55), MultiMapKey.MapStart(Seq(1, 2, 5, 55)), MultiMapKey.MapEnd(Seq(1, 2, 5, 55))),
  //          (MultiMapKey.SubMap(Seq(1, 2, 5, 55), 5555), MultiMapKey.MapStart(Seq(1, 2, 5, 55, 5555)), MultiMapKey.MapEnd(Seq(1, 2, 5, 55, 5555))),
  //          (MultiMapKey.SubMap(Seq(1, 2, 5, 55), 6666), MultiMapKey.MapStart(Seq(1, 2, 5, 55, 6666)), MultiMapKey.MapEnd(Seq(1, 2, 5, 55, 6666))),
  //          (MultiMapKey.SubMap(Seq(1, 2, 5), 555), MultiMapKey.MapStart(Seq(1, 2, 5, 555)), MultiMapKey.MapEnd(Seq(1, 2, 5, 555)))
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
        TestCaseSweeper {
          implicit sweeper =>

            val root = newDB()

            val first = root.schema.init(1)
            val second = first.schema.init(2)
            first.schema.get(2) shouldBe defined
            second.schema.get(2) shouldBe empty
        }
      }
    }

    "children.init on a existing map" should {
      "replace existing map" in {
        TestCaseSweeper {
          implicit sweeper =>

            val root = newDB()

            val first = root.schema.init(1)
            val second = first.schema.init(2)
            val secondAgain = first.schema.replace(2)

            first.schema.get(2) shouldBe defined
        }
      }

      "replace existing map and all it's entries" in {
        TestCaseSweeper {
          implicit sweeper =>

            val root = newDB()

            val first = root.schema.init(1)
            val second = first.schema.init(2)
            //write entries to second map
            second.put(1, "one")
            second.put(2, "two")
            second.put(3, "three")
            //assert second map has these entries
            second.stream.materialize shouldBe List((1, "one"), (2, "two"), (3, "three"))

            val secondAgain = first.schema.replace(2)

            //map value value updated
            first.schema.get(2) shouldBe defined
            //all the old entries are removed
            second.stream.materialize shouldBe empty
        }
      }

      "replace existing map and all it's entries and also all existing maps childMap and all their entries" in {
        TestCaseSweeper {
          implicit sweeper =>
            val root = newDB()

            //MAP HIERARCHY
            //first
            //   second
            //       third
            //           fourth
            val first = root.schema.init(1)
            val second = first.schema.init(2)
            //third map that is the child map of second map
            val third = second.schema.init(3)
            val fourth = third.schema.init(4)

            first.put(1, "first one")
            first.put(2, "first two")
            first.put(3, "first three")

            second.put(1, "second one")
            second.put(2, "second two")
            second.put(3, "second three")

            third.put(1, "third one")
            third.put(2, "third two")
            third.put(3, "third three")

            fourth.put(1, "fourth one")
            fourth.put(2, "fourth two")
            fourth.put(3, "fourth three")

            /**
             * Assert that the all maps' content is accurate
             */
            first.stream.materialize shouldBe List((1, "first one"), (2, "first two"), (3, "first three"))
            second.stream.materialize shouldBe List((1, "second one"), (2, "second two"), (3, "second three"))
            third.stream.materialize shouldBe List((1, "third one"), (2, "third two"), (3, "third three"))
            fourth.stream.materialize shouldBe List((1, "fourth one"), (2, "fourth two"), (3, "fourth three"))

            root.schema.keys.materialize.toList should contain only 1
            first.schema.keys.materialize.toList should contain only 2
            second.schema.keys.materialize.toList should contain only 3
            third.schema.keys.materialize.toList should contain only 4
            fourth.schema.keys.materialize.toList shouldBe empty

            //submit put on second map and assert that all it's contents are replaced.
            first.schema.replace(2)
            first.schema.get(2).value.stream.materialize.toList shouldBe empty
            first.schema.get(2).value.schema.stream.materialize.toList shouldBe empty

            //map value value updated
            first.schema.get(2) shouldBe defined
            //all the old entries are removed
            second.stream.materialize shouldBe empty
            third.stream.materialize shouldBe empty
            fourth.stream.materialize shouldBe empty

            //second has no children anymore.
            second.schema.get(3) shouldBe empty
            second.schema.get(4) shouldBe empty

            first.stream.materialize shouldBe List((1, "first one"), (2, "first two"), (3, "first three"))
        }
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
