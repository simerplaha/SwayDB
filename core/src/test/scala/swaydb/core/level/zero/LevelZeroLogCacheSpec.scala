///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package swaydb.core.level.zero
//
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestData._
//import swaydb.core.TestTimer
//import swaydb.core.data.{Memory, Value}
//import swaydb.core.log.LogEntry
//import swaydb.core.log.serializer.LevelZeroLogEntryWriter
//import swaydb.core.merge.KeyValueMerger
//import swaydb.core.merge.stats.MergeStats
//import swaydb.core.util.skiplist.{SkipListConcurrent, SkipListSeries}
//import swaydb.data.order.TimeOrder
//import swaydb.data.slice.Slice
//import swaydb.data.{Atomic, OptimiseWrites}
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//class LevelZeroLogCacheSpec extends AnyWordSpec with Matchers {
//
//  implicit val keyOrder = swaydb.data.order.KeyOrder.default
//  implicit val testTimer: TestTimer = TestTimer.Empty
//  implicit val timeOrder = TimeOrder.long
//
//  import LevelZeroLogEntryWriter._
//
//  def doInsert(memory: Memory,
//               expectSkipList: Iterable[(Slice[Byte], Memory)])(implicit state: LevelZeroLogCache.State): Unit = {
//
//    LevelZeroMerger.mergeInsert(
//      insert = memory,
//      state = state
//    )
//
//    state.skipList.size shouldBe expectSkipList.size
//
//    state.skipList.toIterable.toList shouldBe expectSkipList
//  }
//
//  "it" should {
//    "create instance with optimised skipLists" in {
//      OptimiseWrites.randomAll foreach {
//        implicit optimiseWrites =>
//
//          Atomic.all foreach {
//            implicit atomic =>
//
//              val cache = LevelZeroLogCache()
//              //there is always at-least one level
//              cache.skipList.iterator should have size 0
//              cache.skipList.size shouldBe 0
//
//              optimiseWrites match {
//                case OptimiseWrites.RandomOrder =>
//                  cache.skipList shouldBe a[SkipListConcurrent[_, _, _, _]]
//
//                case OptimiseWrites.SequentialOrder(_) =>
//                  cache.skipList shouldBe a[SkipListSeries[_, _, _, _]]
//              }
//          }
//      }
//    }
//  }
//
//  "insert" should {
//    "write fixed on fixed" when {
//      "empty" in {
//        OptimiseWrites.randomAll foreach {
//          implicit optimiseWrites =>
//            implicit val state = LevelZeroLogCache.State()
//
//            val put = Memory.put(1)
//
//            doInsert(
//              memory = put,
//              expectSkipList = List((1: Slice[Byte], put))
//            )
//        }
//      }
//
//      "non-empty" when {
//        "overlapping writes" in {
//          OptimiseWrites.randomAll foreach {
//            implicit optimiseWrites =>
//              implicit val state = LevelZeroLogCache.State()
//
//              val put1 = Memory.put(1, 1)
//
//              //first insert
//              doInsert(
//                memory = put1,
//                expectSkipList = List((1: Slice[Byte], put1))
//              )
//
//              val put2 = Memory.put(1, 2)
//
//              doInsert(
//                memory = put2,
//                expectSkipList = List((1: Slice[Byte], put2))
//              )
//          }
//        }
//
//        "non-overlapping writes" in {
//          OptimiseWrites.randomAll foreach {
//            implicit optimiseWrites =>
//              implicit val state = LevelZeroLogCache.State()
//              val put1 = Memory.put(1, 1)
//
//              //first insert
//              doInsert(
//                memory = put1,
//                expectSkipList = List((1: Slice[Byte], put1))
//              )
//
//              val put2 = Memory.put(2, 2)
//
//              doInsert(
//                memory = put2,
//                expectSkipList = List((1: Slice[Byte], put1), (2: Slice[Byte], put2))
//              )
//          }
//        }
//      }
//    }
//
//    "write fixed on range" when {
//      "overlapping fromValue" in {
//        OptimiseWrites.randomAll foreach {
//          implicit optimiseWrites =>
//            implicit val state = LevelZeroLogCache.State()
//
//            val put = Memory.put(1, 1)
//
//            //first insert
//            doInsert(
//              memory = put,
//              expectSkipList = List((1: Slice[Byte], put))
//            )
//
//            val range = Memory.Range(1, 100, randomFromValue(), randomRangeValue())
//
//            val builder = MergeStats.random()
//
//            KeyValueMerger.merge(
//              newKeyValue = range,
//              oldKeyValue = put,
//              builder = builder,
//              isLastLevel = false,
//              initialiseIteratorsInOneSeek = randomBoolean()
//            )
//
//            val result = builder.keyValues.map(memory => (memory.key, memory))
//
//            doInsert(
//              memory = range,
//              expectSkipList = result
//            )
//        }
//      }
//
//      "not overlapping writes" in {
//        OptimiseWrites.randomAll foreach {
//          implicit optimiseWrites =>
//            implicit val state = LevelZeroLogCache.State()
//
//            val put = Memory.put(0, 0)
//
//            //first insert
//            doInsert(
//              memory = put,
//              expectSkipList = List((0: Slice[Byte], put))
//            )
//
//            val range = Memory.Range(1, 100, randomFromValue(), randomRangeValue())
//
//            doInsert(
//              memory = range,
//              expectSkipList = List((0: Slice[Byte], put), (1: Slice[Byte], range))
//            )
//        }
//      }
//    }
//  }
//
//  "writeAtomic" should {
//    implicit def optimiseWrites = OptimiseWrites.random
//
//    implicit def atomic = Atomic.random
//
//    "insert a Fixed value to an empty skipList" in {
//      val cache = LevelZeroLogCache.builder.create()
//
//      val put = Memory.put(1, "one")
//      cache.writeAtomic(LogEntry.Put(put.key, put))
//      cache.skipList.toIterable should have size 1
//
//      cache.skipList.toIterable.head shouldBe ((1: Slice[Byte], put))
//
//      cache.skipList.size shouldBe 1
//    }
//
//    "insert multiple fixed key-values" in {
//
//      val cache = LevelZeroLogCache.builder.create()
//
//      (0 to 9) foreach {
//        i =>
//          cache.writeAtomic(LogEntry.Put(i, Memory.put(i, i)))
//      }
//
//      cache.skipList should have size 10
//
//      cache.skipList.iterator.zipWithIndex foreach {
//        case ((key, value), index) =>
//          key shouldBe (index: Slice[Byte])
//          value shouldBe Memory.put(index, index)
//      }
//    }
//
//    "insert multiple non-overlapping ranges" in {
//      //10 - 20, 30 - 40, 50 - 100
//      //10 - 20, 30 - 40, 50 - 100
//      val cache = LevelZeroLogCache.builder.create()
//
//      cache.writeAtomic(LogEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.remove(None))))
//      cache.writeAtomic(LogEntry.Put(30, Memory.Range(30, 40, Value.FromValue.Null, Value.update(40))))
//      cache.writeAtomic(LogEntry.Put(50, Memory.Range(50, 100, Value.put(20), Value.remove(None))))
//
//      cache.skipList.size shouldBe 3
//
//      val skipListArray = cache.skipList.toIterable.toArray
//      skipListArray(0) shouldBe ((10: Slice[Byte], Memory.Range(10, 20, Value.FromValue.Null, Value.remove(None))))
//      skipListArray(1) shouldBe ((30: Slice[Byte], Memory.Range(30, 40, Value.FromValue.Null, Value.update(40))))
//      skipListArray(2) shouldBe ((50: Slice[Byte], Memory.Range(50, 100, Value.put(20), Value.remove(None))))
//    }
//
//    "insert overlapping ranges when insert fromKey is less than existing range's fromKey" in {
//      //1-15
//      //  20
//      //  10
//
//      //result:
//      //15 | 20
//      //1  | 15
//
//      val cache = LevelZeroLogCache.builder.create()
//
//      cache.writeNonAtomic(LogEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20))))
//      cache.writeNonAtomic(LogEntry.Put(1, Memory.Range(1, 15, Value.FromValue.Null, Value.update(40))))
//      cache.skipList should have size 3
//
//      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 10, Value.FromValue.Null, Value.update(40))
//      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.update(40))
//      cache.skipList.get(15: Slice[Byte]).getS shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
//    }
//
//    "insert overlapping ranges when insert fromKey is less than existing range's from key and fromKey is set" in {
//      //1-15
//      //  20 (R - Put(20)
//      //  10 (Put(10))
//
//      //result:
//      //10 | 15 | 20
//      //1  | 10 | 15
//
//      val cache = LevelZeroLogCache.builder.create()
//
//      //insert with put
//      cache.writeNonAtomic(LogEntry.Put(10, Memory.Range(10, 20, Value.put(10), Value.update(20))))
//      cache.writeNonAtomic(LogEntry.Put(1, Memory.Range(1, 15, Value.FromValue.Null, Value.update(40))))
//      cache.skipList should have size 3
//      val skipListArray = cache.skipList.toIterable.toArray
//
//      skipListArray(0) shouldBe(1: Slice[Byte], Memory.Range(1, 10, Value.FromValue.Null, Value.update(40)))
//      skipListArray(1) shouldBe(10: Slice[Byte], Memory.Range(10, 15, Value.put(40), Value.update(40)))
//      skipListArray(2) shouldBe(15: Slice[Byte], Memory.Range(15, 20, Value.FromValue.Null, Value.update(20)))
//    }
//
//    "insert overlapping ranges when insert fromKey is greater than existing range's fromKey" in {
//      val cache = LevelZeroLogCache.builder.create()
//      //10
//      //1
//      cache.writeNonAtomic(LogEntry.Put(1, Memory.Range(1, 15, Value.FromValue.Null, Value.update(40))))
//      cache.writeNonAtomic(LogEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20))))
//      cache.skipList should have size 3
//
//      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 10, Value.FromValue.Null, Value.update(40))
//      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.update(20))
//      cache.skipList.get(15: Slice[Byte]).getS shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
//    }
//
//    "insert overlapping ranges when insert fromKey is greater than existing range's fromKey and fromKey is set" in {
//      val cache = LevelZeroLogCache.builder.create()
//      //15
//      //1 (Put(1))
//      cache.writeNonAtomic(LogEntry.Put(1, Memory.Range(1, 15, Value.put(1), Value.update(40))))
//      cache.writeNonAtomic(LogEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20))))
//
//      cache.skipList should have size 3
//
//      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 10, Value.put(1), Value.update(40))
//      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.update(20))
//      cache.skipList.get(15: Slice[Byte]).getS shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
//    }
//
//    "insert overlapping ranges without values set and no splits required" in {
//      val cache = LevelZeroLogCache.builder.create()
//      cache.writeNonAtomic(LogEntry.Put(1, Memory.Range(1, 5, Value.FromValue.Null, Value.update(5))))
//      cache.writeNonAtomic(LogEntry.Put(5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))))
//      cache.writeNonAtomic(LogEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20))))
//      cache.writeNonAtomic(LogEntry.Put(20, Memory.Range(20, 30, Value.FromValue.Null, Value.update(30))))
//      cache.writeNonAtomic(LogEntry.Put(30, Memory.Range(30, 40, Value.FromValue.Null, Value.update(40))))
//      cache.writeNonAtomic(LogEntry.Put(40, Memory.Range(40, 50, Value.FromValue.Null, Value.update(50))))
//
//      cache.writeNonAtomic(LogEntry.Put(10, Memory.Range(10, 100, Value.FromValue.Null, Value.update(100))))
//      cache.skipList should have size 7
//
//      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 5, Value.FromValue.Null, Value.update(5))
//      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))
//      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 20, Value.FromValue.Null, Value.update(100))
//      cache.skipList.get(20: Slice[Byte]).getS shouldBe Memory.Range(20, 30, Value.FromValue.Null, Value.update(100))
//      cache.skipList.get(30: Slice[Byte]).getS shouldBe Memory.Range(30, 40, Value.FromValue.Null, Value.update(100))
//      cache.skipList.get(40: Slice[Byte]).getS shouldBe Memory.Range(40, 50, Value.FromValue.Null, Value.update(100))
//      cache.skipList.get(50: Slice[Byte]).getS shouldBe Memory.Range(50, 100, Value.FromValue.Null, Value.update(100))
//    }
//
//    "insert overlapping ranges with values set and no splits required" in {
//      val cache = LevelZeroLogCache.builder.create()
//      cache.writeNonAtomic(LogEntry.Put(1, Memory.Range(1, 5, Value.put(1), Value.update(5))))
//      cache.writeNonAtomic(LogEntry.Put(5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))))
//      cache.writeNonAtomic(LogEntry.Put(10, Memory.Range(10, 20, Value.put(10), Value.update(20))))
//      cache.writeNonAtomic(LogEntry.Put(20, Memory.Range(20, 30, Value.FromValue.Null, Value.update(30))))
//      cache.writeNonAtomic(LogEntry.Put(30, Memory.Range(30, 40, Value.put(30), Value.update(40))))
//      cache.writeNonAtomic(LogEntry.Put(40, Memory.Range(40, 50, Value.FromValue.Null, Value.update(50))))
//
//      cache.writeNonAtomic(LogEntry.Put(10, Memory.Range(10, 100, Value.FromValue.Null, Value.update(100))))
//      cache.skipList should have size 7
//
//      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 5, Value.put(1), Value.update(5))
//      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))
//      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 20, Value.put(100), Value.update(100))
//      cache.skipList.get(20: Slice[Byte]).getS shouldBe Memory.Range(20, 30, Value.FromValue.Null, Value.update(100))
//      cache.skipList.get(30: Slice[Byte]).getS shouldBe Memory.Range(30, 40, Value.put(100), Value.update(100))
//      cache.skipList.get(40: Slice[Byte]).getS shouldBe Memory.Range(40, 50, Value.FromValue.Null, Value.update(100))
//      cache.skipList.get(50: Slice[Byte]).getS shouldBe Memory.Range(50, 100, Value.FromValue.Null, Value.update(100))
//    }
//
//    "insert overlapping ranges with values set and splits required" in {
//      val cache = LevelZeroLogCache.builder.create()
//      cache.writeNonAtomic(LogEntry.Put(1, Memory.Range(1, 5, Value.put(1), Value.update(5))))
//      cache.writeNonAtomic(LogEntry.Put(5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))))
//      cache.writeNonAtomic(LogEntry.Put(10, Memory.Range(10, 20, Value.put(10), Value.update(20))))
//      cache.writeNonAtomic(LogEntry.Put(20, Memory.Range(20, 30, Value.FromValue.Null, Value.update(30))))
//      cache.writeNonAtomic(LogEntry.Put(30, Memory.Range(30, 40, Value.put(30), Value.update(40))))
//      cache.writeNonAtomic(LogEntry.Put(40, Memory.Range(40, 50, Value.FromValue.Null, Value.update(50))))
//
//      cache.writeNonAtomic(LogEntry.Put(7, Memory.Range(7, 35, Value.FromValue.Null, Value.update(100))))
//      cache.skipList should have size 8
//
//      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 5, Value.put(1), Value.update(5))
//      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 7, Value.FromValue.Null, Value.update(10))
//      cache.skipList.get(7: Slice[Byte]).getS shouldBe Memory.Range(7, 10, Value.FromValue.Null, Value.update(100))
//      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 20, Value.put(100), Value.update(100))
//      cache.skipList.get(20: Slice[Byte]).getS shouldBe Memory.Range(20, 30, Value.FromValue.Null, Value.update(100))
//      cache.skipList.get(30: Slice[Byte]).getS shouldBe Memory.Range(30, 35, Value.put(100), Value.update(100))
//      cache.skipList.get(35: Slice[Byte]).getS shouldBe Memory.Range(35, 40, Value.FromValue.Null, Value.update(40))
//      cache.skipList.get(40: Slice[Byte]).getS shouldBe Memory.Range(40, 50, Value.FromValue.Null, Value.update(50))
//    }
//
//    "remove range should remove invalid entries" in {
//      val cache = LevelZeroLogCache.builder.create()
//      cache.writeNonAtomic(LogEntry.Put(1, Memory.put(1, 1)))
//      cache.writeNonAtomic(LogEntry.Put(2, Memory.put(2, 2)))
//      cache.writeNonAtomic(LogEntry.Put(4, Memory.put(4, 4)))
//      cache.writeNonAtomic(LogEntry.Put(5, Memory.put(5, 5)))
//
//      cache.writeNonAtomic(LogEntry.Put(2, Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None))))
//
//      cache.skipList should have size 3
//
//      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.put(1, 1)
//      cache.skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None))
//      cache.skipList.get(3: Slice[Byte]).toOptionS shouldBe empty
//      cache.skipList.get(4: Slice[Byte]).toOptionS shouldBe empty
//      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.put(5, 5)
//
//      cache.writeNonAtomic(LogEntry.Put(5, Memory.remove(5)))
//      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.remove(5)
//
//      cache.skipList should have size 3
//    }
//
//    "remove range when cache.asMergedSkipList is empty" in {
//      val cache = LevelZeroLogCache.builder.create()
//      cache.writeNonAtomic(LogEntry.Put(2, Memory.Range(2, 100, Value.FromValue.Null, Value.remove(None))))
//      cache.skipList should have size 1
//
//      cache.skipList.get(1: Slice[Byte]).toOptionS shouldBe empty
//      cache.skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 100, Value.FromValue.Null, Value.remove(None))
//      cache.skipList.get(3: Slice[Byte]).toOptionS shouldBe empty
//      cache.skipList.get(4: Slice[Byte]).toOptionS shouldBe empty
//    }
//
//    "remove range should clear removed entries when remove ranges overlaps the left edge" in {
//      val cache = LevelZeroLogCache.builder.create()
//      //1           -              10
//      (1 to 10) foreach {
//        i =>
//          cache.writeNonAtomic(LogEntry.Put(i, Memory.put(i, i)))
//      }
//
//      //1           -              10
//      //       4    -      8
//      cache.writeNonAtomic(LogEntry.Put(4, Memory.Range(4, 8, Value.FromValue.Null, Value.remove(None))))
//      cache.writeNonAtomic(LogEntry.Put(8, Memory.remove(8)))
//      //1           -              10
//      //   2    -    5
//      cache.writeNonAtomic(LogEntry.Put(2, Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None))))
//
//      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.put(1, 1)
//      cache.skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 4, Value.FromValue.Null, Value.remove(None))
//      cache.skipList.get(3: Slice[Byte]).toOptionS shouldBe empty
//      cache.skipList.get(4: Slice[Byte]).getS shouldBe Memory.Range(4, 5, Value.FromValue.Null, Value.remove(None))
//      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 8, Value.FromValue.Null, Value.remove(None))
//      cache.skipList.get(6: Slice[Byte]).toOptionS shouldBe empty
//      cache.skipList.get(7: Slice[Byte]).toOptionS shouldBe empty
//      cache.skipList.get(8: Slice[Byte]).getS shouldBe Memory.remove(8)
//      cache.skipList.get(9: Slice[Byte]).getS shouldBe Memory.put(9, 9)
//      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.put(10, 10)
//    }
//
//    "remove range should clear removed entries when remove ranges overlaps the right edge" in {
//      val cache = LevelZeroLogCache.builder.create()
//      //1           -              10
//      (1 to 10) foreach {
//        i =>
//          cache.writeNonAtomic(LogEntry.Put(i, Memory.put(i, i)))
//      }
//      //1           -              10
//      //   2    -    5
//      cache.writeNonAtomic(LogEntry.Put(2, Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None))))
//      cache.writeNonAtomic(LogEntry.Put(5, Memory.remove(5)))
//      //1           -              10
//      //       4    -      8
//      cache.writeNonAtomic(LogEntry.Put(4, Memory.Range(4, 8, Value.FromValue.Null, Value.remove(None))))
//      //      cache.write(LogEntry.Put(8, Memory.remove(8)))
//
//      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.put(1, 1)
//      cache.skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 4, Value.FromValue.Null, Value.remove(None))
//      cache.skipList.get(3: Slice[Byte]).toOptionS shouldBe empty
//      cache.skipList.get(4: Slice[Byte]).getS shouldBe Memory.Range(4, 5, Value.FromValue.Null, Value.remove(None))
//      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 8, Value.FromValue.Null, Value.remove(None))
//      cache.skipList.get(6: Slice[Byte]).toOptionS shouldBe empty
//      cache.skipList.get(7: Slice[Byte]).toOptionS shouldBe empty
//      cache.skipList.get(8: Slice[Byte]).getS shouldBe Memory.put(8, 8)
//      cache.skipList.get(9: Slice[Byte]).getS shouldBe Memory.put(9, 9)
//      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.put(10, 10)
//    }
//
//    "insert fixed key-values into remove range" in {
//      val cache = LevelZeroLogCache.builder.create()
//      //1           -              10
//      cache.writeNonAtomic(LogEntry.Put(1, Memory.Range(1, 10, Value.FromValue.Null, Value.remove(None))))
//      (1 to 10) foreach {
//        i =>
//          cache.writeNonAtomic(LogEntry.Put(i, Memory.put(i, i)))
//      }
//
//      cache.skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 2, Value.put(1), Value.remove(None))
//      cache.skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 3, Value.put(2), Value.remove(None))
//      cache.skipList.get(3: Slice[Byte]).getS shouldBe Memory.Range(3, 4, Value.put(3), Value.remove(None))
//      cache.skipList.get(4: Slice[Byte]).getS shouldBe Memory.Range(4, 5, Value.put(4), Value.remove(None))
//      cache.skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 6, Value.put(5), Value.remove(None))
//      cache.skipList.get(6: Slice[Byte]).getS shouldBe Memory.Range(6, 7, Value.put(6), Value.remove(None))
//      cache.skipList.get(7: Slice[Byte]).getS shouldBe Memory.Range(7, 8, Value.put(7), Value.remove(None))
//      cache.skipList.get(8: Slice[Byte]).getS shouldBe Memory.Range(8, 9, Value.put(8), Value.remove(None))
//      cache.skipList.get(9: Slice[Byte]).getS shouldBe Memory.Range(9, 10, Value.put(9), Value.remove(None))
//      cache.skipList.get(10: Slice[Byte]).getS shouldBe Memory.put(10, 10)
//    }
//  }
//}
