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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */
package swaydb.core.level.zero

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.core.data.{Memory, Value}
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.LevelZeroMapEntryWriter
import swaydb.core.segment.merge.{MergeStats, SegmentMerger}
import swaydb.core.util.skiplist.{SkipListConcurrent, SkipListSeries}
import swaydb.data.OptimiseWrites
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class LevelZeroMapCacheSpec extends AnyWordSpec with Matchers {

  implicit val keyOrder = swaydb.data.order.KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder = TimeOrder.long

  import LevelZeroMapEntryWriter._

  def doInsert(memory: Memory,
               expectSkipList: Iterable[(Slice[Byte], Memory)])(implicit state: LevelZeroMapCache.State): Unit = {

    LevelZeroMapCache.insert(
      insert = memory,
      state = state
    )

    state.skipList.size shouldBe expectSkipList.size

    state.skipList.toIterable.toList shouldBe expectSkipList
  }

  "it" should {
    "create instance with optimised skipLists" in {
      OptimiseWrites.randomAll foreach {
        implicit optimiseWrites =>
          val cache = LevelZeroMapCache()
          //there is always at-least one level
          cache.skipList.iterator should have size 0
          cache.skipList.size shouldBe 0

          optimiseWrites match {
            case OptimiseWrites.RandomOrder(_) =>
              cache.skipList shouldBe a[SkipListConcurrent[_, _, _, _]]

            case OptimiseWrites.SequentialOrder(_, _) =>
              cache.skipList shouldBe a[SkipListSeries[_, _, _, _]]
          }
      }
    }
  }

  "insert" should {
    "write fixed on fixed" when {
      "empty" in {
        OptimiseWrites.randomAll foreach {
          implicit optimiseWrites =>
            implicit val state = LevelZeroMapCache.State()

            val put = Memory.put(1)

            doInsert(
              memory = put,
              expectSkipList = List((1: Slice[Byte], put))
            )
        }
      }

      "non-empty" when {
        "overlapping writes" in {
          OptimiseWrites.randomAll foreach {
            implicit optimiseWrites =>
              implicit val state = LevelZeroMapCache.State()

              val put1 = Memory.put(1, 1)

              //first insert
              doInsert(
                memory = put1,
                expectSkipList = List((1: Slice[Byte], put1))
              )

              val put2 = Memory.put(1, 2)

              doInsert(
                memory = put2,
                expectSkipList = List((1: Slice[Byte], put2))
              )
          }
        }

        "non-overlapping writes" in {
          OptimiseWrites.randomAll foreach {
            implicit optimiseWrites =>
              implicit val state = LevelZeroMapCache.State()
              val put1 = Memory.put(1, 1)

              //first insert
              doInsert(
                memory = put1,
                expectSkipList = List((1: Slice[Byte], put1))
              )

              val put2 = Memory.put(2, 2)

              doInsert(
                memory = put2,
                expectSkipList = List((1: Slice[Byte], put1), (2: Slice[Byte], put2))
              )
          }
        }
      }
    }

    "write fixed on range" when {
      "overlapping fromValue" in {
        OptimiseWrites.randomAll foreach {
          implicit optimiseWrites =>
            implicit val state = LevelZeroMapCache.State()

            val put = Memory.put(1, 1)

            //first insert
            doInsert(
              memory = put,
              expectSkipList = List((1: Slice[Byte], put))
            )

            val range = Memory.Range(1, 100, randomFromValue(), randomRangeValue())

            val builder = MergeStats.random()

            SegmentMerger.merge(
              newKeyValue = range,
              oldKeyValue = put,
              builder = builder,
              isLastLevel = false
            )

            val result = builder.keyValues.map(memory => (memory.key, memory))

            doInsert(
              memory = range,
              expectSkipList = result
            )
        }
      }

      "not overlapping writes" in {
        OptimiseWrites.randomAll foreach {
          implicit optimiseWrites =>
            implicit val state = LevelZeroMapCache.State()

            val put = Memory.put(0, 0)

            //first insert
            doInsert(
              memory = put,
              expectSkipList = List((0: Slice[Byte], put))
            )

            val range = Memory.Range(1, 100, randomFromValue(), randomRangeValue())

            doInsert(
              memory = range,
              expectSkipList = List((0: Slice[Byte], put), (1: Slice[Byte], range))
            )
        }
      }
    }
  }

  "writeAtomic" should {
    implicit def optimiseWrites = OptimiseWrites.random

    "insert a Fixed value to an empty skipList" in {
      val cache = LevelZeroMapCache.builder.create()

      val put = Memory.put(1, "one")
      cache.writeAtomic(MapEntry.Put(put.key, put))
      cache.skipList.toIterable should have size 1

      cache.skipList.toIterable.head shouldBe ((1: Slice[Byte], put))

      cache.skipList.size shouldBe 1
    }

    "insert multiple fixed key-values" in {

      val cache = LevelZeroMapCache.builder.create()

      (0 to 9) foreach {
        i =>
          cache.writeAtomic(MapEntry.Put(i, Memory.put(i, i)))
      }

      cache.skipList should have size 10

      cache.skipList.iterator.zipWithIndex foreach {
        case ((key, value), index) =>
          key shouldBe (index: Slice[Byte])
          value shouldBe Memory.put(index, index)
      }
    }

    "insert multiple non-overlapping ranges" in {
      //10 - 20, 30 - 40, 50 - 100
      //10 - 20, 30 - 40, 50 - 100
      val cache = LevelZeroMapCache.builder.create()

      cache.writeAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.remove(None))))
      cache.writeAtomic(MapEntry.Put(30, Memory.Range(30, 40, Value.FromValue.Null, Value.update(40))))
      cache.writeAtomic(MapEntry.Put(50, Memory.Range(50, 100, Value.put(20), Value.remove(None))))

      cache.skipList.size shouldBe 3

      val skipListArray = cache.skipList.toIterable.toArray
      skipListArray(0) shouldBe ((10: Slice[Byte], Memory.Range(10, 20, Value.FromValue.Null, Value.remove(None))))
      skipListArray(1) shouldBe ((30: Slice[Byte], Memory.Range(30, 40, Value.FromValue.Null, Value.update(40))))
      skipListArray(2) shouldBe ((50: Slice[Byte], Memory.Range(50, 100, Value.put(20), Value.remove(None))))
    }

    "insert overlapping ranges when insert fromKey is less than existing range's fromKey" in {
      //1-15
      //  20
      //  10

      //result:
      //15 | 20
      //1  | 15

      val cache = LevelZeroMapCache.builder.create()

      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20))))
      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 15, Value.FromValue.Null, Value.update(40))))
      cache.skipList should have size 3

      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 10, Value.FromValue.Null, Value.update(40))
      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.update(40))
      cache.skipList.get(15: Slice[Byte]).getS shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
    }

    "insert overlapping ranges when insert fromKey is less than existing range's from key and fromKey is set" in {
      //1-15
      //  20 (R - Put(20)
      //  10 (Put(10))

      //result:
      //10 | 15 | 20
      //1  | 10 | 15

      val cache = LevelZeroMapCache.builder.create()

      //insert with put
      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.put(10), Value.update(20))))
      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 15, Value.FromValue.Null, Value.update(40))))
      cache.skipList should have size 3
      val skipListArray = cache.skipList.toIterable.toArray

      skipListArray(0) shouldBe(1: Slice[Byte], Memory.Range(1, 10, Value.FromValue.Null, Value.update(40)))
      skipListArray(1) shouldBe(10: Slice[Byte], Memory.Range(10, 15, Value.put(40), Value.update(40)))
      skipListArray(2) shouldBe(15: Slice[Byte], Memory.Range(15, 20, Value.FromValue.Null, Value.update(20)))
    }

    "insert overlapping ranges when insert fromKey is greater than existing range's fromKey" in {
      val cache = LevelZeroMapCache.builder.create()
      //10
      //1
      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 15, Value.FromValue.Null, Value.update(40))))
      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20))))
      cache.skipList should have size 3

      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 10, Value.FromValue.Null, Value.update(40))
      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.update(20))
      cache.skipList.get(15: Slice[Byte]).getS shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
    }

    "insert overlapping ranges when insert fromKey is greater than existing range's fromKey and fromKey is set" in {
      val cache = LevelZeroMapCache.builder.create()
      //15
      //1 (Put(1))
      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 15, Value.put(1), Value.update(40))))
      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20))))

      cache.skipList should have size 3

      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 10, Value.put(1), Value.update(40))
      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.update(20))
      cache.skipList.get(15: Slice[Byte]).getS shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
    }

    "insert overlapping ranges without values set and no splits required" in {
      val cache = LevelZeroMapCache.builder.create()
      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 5, Value.FromValue.Null, Value.update(5))))
      cache.writeNonAtomic(MapEntry.Put(5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))))
      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20))))
      cache.writeNonAtomic(MapEntry.Put(20, Memory.Range(20, 30, Value.FromValue.Null, Value.update(30))))
      cache.writeNonAtomic(MapEntry.Put(30, Memory.Range(30, 40, Value.FromValue.Null, Value.update(40))))
      cache.writeNonAtomic(MapEntry.Put(40, Memory.Range(40, 50, Value.FromValue.Null, Value.update(50))))

      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 100, Value.FromValue.Null, Value.update(100))))
      cache.skipList should have size 7

      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 5, Value.FromValue.Null, Value.update(5))
      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))
      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 20, Value.FromValue.Null, Value.update(100))
      cache.skipList.get(20: Slice[Byte]).getS shouldBe Memory.Range(20, 30, Value.FromValue.Null, Value.update(100))
      cache.skipList.get(30: Slice[Byte]).getS shouldBe Memory.Range(30, 40, Value.FromValue.Null, Value.update(100))
      cache.skipList.get(40: Slice[Byte]).getS shouldBe Memory.Range(40, 50, Value.FromValue.Null, Value.update(100))
      cache.skipList.get(50: Slice[Byte]).getS shouldBe Memory.Range(50, 100, Value.FromValue.Null, Value.update(100))
    }

    "insert overlapping ranges with values set and no splits required" in {
      val cache = LevelZeroMapCache.builder.create()
      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 5, Value.put(1), Value.update(5))))
      cache.writeNonAtomic(MapEntry.Put(5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))))
      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.put(10), Value.update(20))))
      cache.writeNonAtomic(MapEntry.Put(20, Memory.Range(20, 30, Value.FromValue.Null, Value.update(30))))
      cache.writeNonAtomic(MapEntry.Put(30, Memory.Range(30, 40, Value.put(30), Value.update(40))))
      cache.writeNonAtomic(MapEntry.Put(40, Memory.Range(40, 50, Value.FromValue.Null, Value.update(50))))

      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 100, Value.FromValue.Null, Value.update(100))))
      cache.skipList should have size 7

      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 5, Value.put(1), Value.update(5))
      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))
      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 20, Value.put(100), Value.update(100))
      cache.skipList.get(20: Slice[Byte]).getS shouldBe Memory.Range(20, 30, Value.FromValue.Null, Value.update(100))
      cache.skipList.get(30: Slice[Byte]).getS shouldBe Memory.Range(30, 40, Value.put(100), Value.update(100))
      cache.skipList.get(40: Slice[Byte]).getS shouldBe Memory.Range(40, 50, Value.FromValue.Null, Value.update(100))
      cache.skipList.get(50: Slice[Byte]).getS shouldBe Memory.Range(50, 100, Value.FromValue.Null, Value.update(100))
    }

    "insert overlapping ranges with values set and splits required" in {
      val cache = LevelZeroMapCache.builder.create()
      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 5, Value.put(1), Value.update(5))))
      cache.writeNonAtomic(MapEntry.Put(5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))))
      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.put(10), Value.update(20))))
      cache.writeNonAtomic(MapEntry.Put(20, Memory.Range(20, 30, Value.FromValue.Null, Value.update(30))))
      cache.writeNonAtomic(MapEntry.Put(30, Memory.Range(30, 40, Value.put(30), Value.update(40))))
      cache.writeNonAtomic(MapEntry.Put(40, Memory.Range(40, 50, Value.FromValue.Null, Value.update(50))))

      cache.writeNonAtomic(MapEntry.Put(7, Memory.Range(7, 35, Value.FromValue.Null, Value.update(100))))
      cache.skipList should have size 8

      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 5, Value.put(1), Value.update(5))
      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 7, Value.FromValue.Null, Value.update(10))
      cache.skipList.get(7: Slice[Byte]).getS shouldBe Memory.Range(7, 10, Value.FromValue.Null, Value.update(100))
      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 20, Value.put(100), Value.update(100))
      cache.skipList.get(20: Slice[Byte]).getS shouldBe Memory.Range(20, 30, Value.FromValue.Null, Value.update(100))
      cache.skipList.get(30: Slice[Byte]).getS shouldBe Memory.Range(30, 35, Value.put(100), Value.update(100))
      cache.skipList.get(35: Slice[Byte]).getS shouldBe Memory.Range(35, 40, Value.FromValue.Null, Value.update(40))
      cache.skipList.get(40: Slice[Byte]).getS shouldBe Memory.Range(40, 50, Value.FromValue.Null, Value.update(50))
    }

    "remove range should remove invalid entries" in {
      val cache = LevelZeroMapCache.builder.create()
      cache.writeNonAtomic(MapEntry.Put(1, Memory.put(1, 1)))
      cache.writeNonAtomic(MapEntry.Put(2, Memory.put(2, 2)))
      cache.writeNonAtomic(MapEntry.Put(4, Memory.put(4, 4)))
      cache.writeNonAtomic(MapEntry.Put(5, Memory.put(5, 5)))

      cache.writeNonAtomic(MapEntry.Put(2, Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None))))

      cache.skipList should have size 3

      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.put(1, 1)
      cache.skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None))
      cache.skipList.get(3: Slice[Byte]).toOptionS shouldBe empty
      cache.skipList.get(4: Slice[Byte]).toOptionS shouldBe empty
      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.put(5, 5)

      cache.writeNonAtomic(MapEntry.Put(5, Memory.remove(5)))
      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.remove(5)

      cache.skipList should have size 3
    }

    "remove range when cache.asMergedSkipList is empty" in {
      val cache = LevelZeroMapCache.builder.create()
      cache.writeNonAtomic(MapEntry.Put(2, Memory.Range(2, 100, Value.FromValue.Null, Value.remove(None))))
      cache.skipList should have size 1

      cache.skipList.get(1: Slice[Byte]).toOptionS shouldBe empty
      cache.skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 100, Value.FromValue.Null, Value.remove(None))
      cache.skipList.get(3: Slice[Byte]).toOptionS shouldBe empty
      cache.skipList.get(4: Slice[Byte]).toOptionS shouldBe empty
    }

    "remove range should clear removed entries when remove ranges overlaps the left edge" in {
      val cache = LevelZeroMapCache.builder.create()
      //1           -              10
      (1 to 10) foreach {
        i =>
          cache.writeNonAtomic(MapEntry.Put(i, Memory.put(i, i)))
      }

      //1           -              10
      //       4    -      8
      cache.writeNonAtomic(MapEntry.Put(4, Memory.Range(4, 8, Value.FromValue.Null, Value.remove(None))))
      cache.writeNonAtomic(MapEntry.Put(8, Memory.remove(8)))
      //1           -              10
      //   2    -    5
      cache.writeNonAtomic(MapEntry.Put(2, Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None))))

      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.put(1, 1)
      cache.skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 4, Value.FromValue.Null, Value.remove(None))
      cache.skipList.get(3: Slice[Byte]).toOptionS shouldBe empty
      cache.skipList.get(4: Slice[Byte]).getS shouldBe Memory.Range(4, 5, Value.FromValue.Null, Value.remove(None))
      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 8, Value.FromValue.Null, Value.remove(None))
      cache.skipList.get(6: Slice[Byte]).toOptionS shouldBe empty
      cache.skipList.get(7: Slice[Byte]).toOptionS shouldBe empty
      cache.skipList.get(8: Slice[Byte]).getS shouldBe Memory.remove(8)
      cache.skipList.get(9: Slice[Byte]).getS shouldBe Memory.put(9, 9)
      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.put(10, 10)
    }

    "remove range should clear removed entries when remove ranges overlaps the right edge" in {
      val cache = LevelZeroMapCache.builder.create()
      //1           -              10
      (1 to 10) foreach {
        i =>
          cache.writeNonAtomic(MapEntry.Put(i, Memory.put(i, i)))
      }
      //1           -              10
      //   2    -    5
      cache.writeNonAtomic(MapEntry.Put(2, Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None))))
      cache.writeNonAtomic(MapEntry.Put(5, Memory.remove(5)))
      //1           -              10
      //       4    -      8
      cache.writeNonAtomic(MapEntry.Put(4, Memory.Range(4, 8, Value.FromValue.Null, Value.remove(None))))
      //      cache.write(MapEntry.Put(8, Memory.remove(8)))

      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.put(1, 1)
      cache.skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 4, Value.FromValue.Null, Value.remove(None))
      cache.skipList.get(3: Slice[Byte]).toOptionS shouldBe empty
      cache.skipList.get(4: Slice[Byte]).getS shouldBe Memory.Range(4, 5, Value.FromValue.Null, Value.remove(None))
      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 8, Value.FromValue.Null, Value.remove(None))
      cache.skipList.get(6: Slice[Byte]).toOptionS shouldBe empty
      cache.skipList.get(7: Slice[Byte]).toOptionS shouldBe empty
      cache.skipList.get(8: Slice[Byte]).getS shouldBe Memory.put(8, 8)
      cache.skipList.get(9: Slice[Byte]).getS shouldBe Memory.put(9, 9)
      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.put(10, 10)
    }

    "insert fixed key-values into remove range" in {
      val cache = LevelZeroMapCache.builder.create()
      //1           -              10
      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 10, Value.FromValue.Null, Value.remove(None))))
      (1 to 10) foreach {
        i =>
          cache.writeNonAtomic(MapEntry.Put(i, Memory.put(i, i)))
      }

      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 2, Value.put(1), Value.remove(None))
      cache.skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 3, Value.put(2), Value.remove(None))
      cache.skipList.get(3: Slice[Byte]).getS shouldBe Memory.Range(3, 4, Value.put(3), Value.remove(None))
      cache.skipList.get(4: Slice[Byte]).getS shouldBe Memory.Range(4, 5, Value.put(4), Value.remove(None))
      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 6, Value.put(5), Value.remove(None))
      cache.skipList.get(6: Slice[Byte]).getS shouldBe Memory.Range(6, 7, Value.put(6), Value.remove(None))
      cache.skipList.get(7: Slice[Byte]).getS shouldBe Memory.Range(7, 8, Value.put(7), Value.remove(None))
      cache.skipList.get(8: Slice[Byte]).getS shouldBe Memory.Range(8, 9, Value.put(8), Value.remove(None))
      cache.skipList.get(9: Slice[Byte]).getS shouldBe Memory.Range(9, 10, Value.put(9), Value.remove(None))
      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.put(10, 10)
    }
  }
}
