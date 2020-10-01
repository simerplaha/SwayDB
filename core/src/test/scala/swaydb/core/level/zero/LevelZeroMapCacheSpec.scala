///*
// * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// *
// * Additional permission under the GNU Affero GPL version 3 section 7:
// * If you modify this Program or any covered work, only by linking or
// * combining it with separate works, the licensors of this Program grant
// * you additional permission to convey the resulting work.
// */
//package swaydb.core.level.zero
//
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestData._
//import swaydb.core.TestTimer
//import swaydb.core.data.Value.FromValue
//import swaydb.core.data.{Memory, Time, Value}
//import swaydb.core.map.MapEntry
//import swaydb.core.map.serializer.LevelZeroMapEntryWriter
//import swaydb.core.segment.merge.{MergeStats, SegmentMerger}
//import swaydb.core.util.skiplist.{SkipListConcurrent, SkipListSeries}
//import swaydb.data.OptimiseWrites
//import swaydb.data.RunThis._
//import swaydb.data.order.TimeOrder
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//class LevelZeroMapCacheSpec extends AnyWordSpec with Matchers {
//
//  implicit val keyOrder = swaydb.data.order.KeyOrder.default
//  implicit val testTimer: TestTimer = TestTimer.Empty
//  implicit val timeOrder = TimeOrder.long
//
//  implicit def optimiseWrites: OptimiseWrites = OptimiseWrites.random
//
//  import LevelZeroMapEntryWriter._
//
//  def doInsert(memory: Memory,
//               atomic: Boolean,
//               expectInserted: Boolean,
//               expectSkipList: Iterable[(Slice[Byte], Memory)],
//               level: LevelZeroMapCache.LevelEmbedded = LevelZeroMapCache.newLevel()): LevelZeroMapCache.LevelEmbedded = {
//
//    LevelZeroMapCache.insert(
//      insert = memory,
//      level = level,
//      atomic = atomic
//    ) shouldBe expectInserted
//
//    level.skipList.size shouldBe expectSkipList.size
//
//    level.skipList.toIterable.toList shouldBe expectSkipList
//    level
//  }
//
//  "it" should {
//    "create instance with optimised skipLists" in {
//      Seq(
//        OptimiseWrites.SequentialOrder(transactionQueueMaxSize = 10, initialSkipListLength = 10),
//        OptimiseWrites.RandomOrder(transactionQueueMaxSize = 10)
//      ) foreach {
//        implicit optimiseWrites =>
//          val cache = LevelZeroMapCache()
//          //there is always at-least one level
//          cache.levels.iterator should have size 1
//          cache.flatten.fetch.size shouldBe 0
//
//          optimiseWrites match {
//            case OptimiseWrites.RandomOrder(_) =>
//              cache.levels.headOrNull().skipList shouldBe a[SkipListConcurrent[_, _, _, _]]
//
//            case OptimiseWrites.SequentialOrder(_, _) =>
//              cache.levels.headOrNull().skipList shouldBe a[SkipListSeries[_, _, _, _]]
//          }
//      }
//    }
//  }
//
//  "insert" should {
//    "write fixed on fixed" when {
//      "empty" should {
//        "succeed - atomic or non-atomic" in {
//          Seq(true, false) foreach {
//            atomic =>
//              val put = Memory.put(1)
//
//              doInsert(
//                memory = put,
//                atomic = atomic,
//                expectInserted = true,
//                expectSkipList = List((1: Slice[Byte], put))
//              )
//          }
//        }
//      }
//
//      "non-empty" should {
//        "succeed" when {
//          "overlapping writes - atomic or non-atomic" in {
//            Seq(true, false) foreach {
//              atomic =>
//                val put1 = Memory.put(1, 1)
//
//                //first insert
//                val level =
//                  doInsert(
//                    memory = put1,
//                    atomic = atomic,
//                    expectInserted = true,
//                    expectSkipList = List((1: Slice[Byte], put1))
//                  )
//
//                val put2 = Memory.put(1, 2)
//
//                doInsert(
//                  memory = put2,
//                  atomic = atomic,
//                  expectInserted = true,
//                  expectSkipList = List((1: Slice[Byte], put2)),
//                  level = level
//                )
//            }
//          }
//
//          "non-overlapping writes - atomic or non-atomic" in {
//            Seq(true, false) foreach {
//              atomic =>
//                val put1 = Memory.put(1, 1)
//
//                //first insert
//                val level =
//                  doInsert(
//                    memory = put1,
//                    atomic = atomic,
//                    expectInserted = true,
//                    expectSkipList = List((1: Slice[Byte], put1))
//                  )
//
//                val put2 = Memory.put(2, 2)
//
//                doInsert(
//                  memory = put2,
//                  atomic = atomic,
//                  expectInserted = true,
//                  expectSkipList = List((1: Slice[Byte], put1), (2: Slice[Byte], put2)),
//                  level = level
//                )
//            }
//          }
//        }
//      }
//    }
//
//    "write fixed on range" should {
//      "succeed" when {
//        "overlapping fromValue - atomic or non-atomic" in {
//          Seq((1, true), (1, false)) foreach {
//            case (putKey, atomic) =>
//              println(s"putKey: $putKey, atomic: $atomic")
//
//              val put = Memory.put(putKey, 1)
//
//              //first insert
//              val level =
//                doInsert(
//                  memory = put,
//                  atomic = atomic,
//                  expectInserted = true,
//                  expectSkipList = List((putKey: Slice[Byte], put))
//                )
//
//              val range = Memory.Range(1, 100, randomFromValue(), randomRangeValue())
//
//              val builder = MergeStats.random()
//
//              SegmentMerger.merge(
//                newKeyValue = range,
//                oldKeyValue = put,
//                builder = builder,
//                isLastLevel = false
//              )
//
//              val result = builder.keyValues.map(memory => (memory.key, memory))
//
//              doInsert(
//                memory = range,
//                atomic = atomic,
//                expectInserted = true,
//                expectSkipList = result,
//                level = level
//              )
//          }
//        }
//
//        "not overlapping writes - atomic or non-atomic" in {
//          Seq(true, false) foreach {
//            atomic =>
//              val put = Memory.put(0, 0)
//
//              //first insert
//              val level =
//                doInsert(
//                  memory = put,
//                  atomic = atomic,
//                  expectInserted = true,
//                  expectSkipList = List((0: Slice[Byte], put))
//                )
//
//              val range = Memory.Range(1, 100, randomFromValue(), randomRangeValue())
//
//              doInsert(
//                memory = range,
//                atomic = atomic,
//                expectInserted = true,
//                expectSkipList = List((0: Slice[Byte], put), (1: Slice[Byte], range)),
//                level = level
//              )
//          }
//        }
//      }
//
//      "fail" when {
//        "overlapping toValue and atomic" in {
//          runThis(100.times, log = true) {
//            Seq(2, 9, 10, 99) foreach {
//              putKey =>
//
//                val put = Memory.put(putKey, putKey)
//
//                println(s"putKey: $putKey - $put")
//
//                //first insert
//                val level =
//                  doInsert(
//                    memory = put,
//                    atomic = true,
//                    expectInserted = true,
//                    expectSkipList = List((putKey: Slice[Byte], put))
//                  )
//
//                val range = Memory.Range(1, 100, randomFromValue(), randomRangeValue())
//
//                //expect state remains unchanged because atomic is fails
//                doInsert(
//                  memory = range,
//                  atomic = true,
//                  expectInserted = false,
//                  expectSkipList = List((put.key, put)),
//                  level = level
//                )
//            }
//          }
//        }
//      }
//
//      "fail" when {
//        "overlapping toValue and not-atomic" in {
//          runThis(100.times) {
//            Seq(2, 9, 10, 99) foreach {
//              putKey =>
//                println(s"putKey: $putKey")
//
//                val put = Memory.put(putKey, 1)
//
//                //first insert
//                val level =
//                  doInsert(
//                    memory = put,
//                    atomic = true,
//                    expectInserted = true,
//                    expectSkipList = List((putKey: Slice[Byte], put))
//                  )
//
//                val range = Memory.Range(1, 100, randomFromValue(), randomRangeValue())
//
//                val builder = MergeStats.random()
//
//                SegmentMerger.merge(
//                  newKeyValue = range,
//                  oldKeyValue = put,
//                  builder = builder,
//                  isLastLevel = false
//                )
//
//                val result = builder.keyValues.map(memory => (memory.key, memory))
//
//                //expect state changed since writes are not atomic
//                doInsert(
//                  memory = range,
//                  atomic = false,
//                  expectInserted = true,
//                  expectSkipList = result,
//                  level = level
//                )
//            }
//          }
//        }
//      }
//    }
//  }
//
//  "merge" should {
//    "combine two levels" when {
//      "overlapping fixed key-values" in {
//        val put = Memory.put(1)
//
//        val older =
//          doInsert(
//            memory = put,
//            atomic = true,
//            expectInserted = true,
//            expectSkipList = List((put.key, put))
//          )
//
//        val newer =
//          doInsert(
//            memory = put,
//            atomic = true,
//            expectInserted = true,
//            expectSkipList = List((put.key, put))
//          )
//
//        val merged = LevelZeroMapCache.merge(newer, older)
//        merged.hasRange shouldBe false
//
//        merged.skipList.toIterable.toList shouldBe List((put.key, put))
//      }
//
//      "non-overlapping fixed key-values" in {
//        val put1 = Memory.put(1)
//
//        val older =
//          doInsert(
//            memory = put1,
//            atomic = true,
//            expectInserted = true,
//            expectSkipList = List((put1.key, put1))
//          )
//
//        val put2 = Memory.put(2)
//
//        val newer =
//          doInsert(
//            memory = put2,
//            atomic = true,
//            expectInserted = true,
//            expectSkipList = List((put2.key, put2))
//          )
//
//        val merged = LevelZeroMapCache.merge(newer, older)
//        merged.hasRange shouldBe false
//
//        merged.skipList.toIterable.toList shouldBe List((put1.key, put1), (put2.key, put2))
//      }
//
//      "overlapping range key-values" in {
//        Seq(true, false) foreach {
//          atomic =>
//            println(s"Atomic: $atomic")
//
//            val range1 = Memory.Range(1, 100, randomFromValue(), randomRangeValue())
//
//            val older =
//              doInsert(
//                memory = range1,
//                atomic = atomic,
//                expectInserted = true,
//                expectSkipList = List((range1.key, range1))
//              )
//
//            val range2 = Memory.Range(50, 200, randomFromValue(), randomRangeValue())
//
//            val newer =
//              doInsert(
//                memory = range2,
//                atomic = atomic,
//                expectInserted = true,
//                expectSkipList = List((range2.key, range2))
//              )
//
//            val merged = LevelZeroMapCache.merge(newer, older)
//
//            merged.hasRange shouldBe true
//
//            val builder = MergeStats.random()
//
//            SegmentMerger.merge(
//              newKeyValue = range2,
//              oldKeyValue = range1,
//              builder = builder,
//              isLastLevel = false
//            )
//
//            val result = builder.keyValues.map(memory => (memory.key, memory))
//
//            merged.skipList.toIterable.toList shouldBe result
//        }
//      }
//
//      "overlapping fixed & range key-values" in {
//        Seq(true, false) foreach {
//          atomic =>
//            println(s"Atomic: $atomic")
//
//            val put1 = Memory.put(100)
//
//            val older =
//              doInsert(
//                memory = put1,
//                atomic = atomic,
//                expectInserted = true,
//                expectSkipList = List((put1.key, put1))
//              )
//
//            older.hasRange shouldBe false
//
//            val range2 = Memory.Range(50, 200, randomFromValue(), randomRangeValue())
//
//            val newer =
//              doInsert(
//                memory = range2,
//                atomic = atomic,
//                expectInserted = true,
//                expectSkipList = List((range2.key, range2))
//              )
//
//            newer.hasRange shouldBe true
//
//            val merged = LevelZeroMapCache.merge(newer, older)
//
//            merged.hasRange shouldBe true
//
//            val builder = MergeStats.random()
//
//            SegmentMerger.merge(
//              newKeyValue = range2,
//              oldKeyValue = put1,
//              builder = builder,
//              isLastLevel = false
//            )
//
//            val result = builder.keyValues.map(memory => (memory.key, memory))
//
//            merged.skipList.toIterable.toList shouldBe result
//        }
//      }
//    }
//  }
//
//  "writeAtomic" should {
//    "insert a Fixed value to an empty skipList" in {
//      val cache = LevelZeroMapCache.builder.create()
//
//      val put = Memory.put(1, "one")
//      cache.writeAtomic(MapEntry.Put(put.key, put))
//      cache.flattenClear should have size 1
//
//      cache.flattenClear.toIterable.head shouldBe ((1: Slice[Byte], put))
//
//      cache.levels.size shouldBe 1
//    }
//
//    "insert multiple fixed key-values" in {
//
//      val cache = LevelZeroMapCache.builder.create()
//
//      (0 to 9) foreach {
//        i =>
//          cache.writeAtomic(MapEntry.Put(i, Memory.put(i, i)))
//      }
//
//      cache.levels.size shouldBe 1
//
//      cache.flattenClear should have size 10
//
//      cache.flattenClear.iterator.zipWithIndex foreach {
//        case ((key, value), index) =>
//          key shouldBe (index: Slice[Byte])
//          value shouldBe Memory.put(index, index)
//      }
//    }
//
//    "insert multiple non-overlapping ranges" in {
//      //10 - 20, 30 - 40, 50 - 100
//      //10 - 20, 30 - 40, 50 - 100
//      val cache = LevelZeroMapCache.builder.create()
//
//      cache.writeAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.remove(None))))
//      cache.writeAtomic(MapEntry.Put(30, Memory.Range(30, 40, Value.FromValue.Null, Value.update(40))))
//      cache.writeAtomic(MapEntry.Put(50, Memory.Range(50, 100, Value.put(20), Value.remove(None))))
//
//      cache.levels.size shouldBe 1
//
//      val skipListArray = cache.flattenClear.toIterable.toArray
//      skipListArray(0) shouldBe ((10: Slice[Byte], Memory.Range(10, 20, Value.FromValue.Null, Value.remove(None))))
//      skipListArray(1) shouldBe ((30: Slice[Byte], Memory.Range(30, 40, Value.FromValue.Null, Value.update(40))))
//      skipListArray(2) shouldBe ((50: Slice[Byte], Memory.Range(50, 100, Value.put(20), Value.remove(None))))
//
//      cache.levels.size shouldBe 1
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
//      val cache = LevelZeroMapCache.builder.create()
//
//      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20))))
//      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 15, Value.FromValue.Null, Value.update(40))))
//      cache.flattenClear should have size 3
//
//      cache.flattenClear.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 10, Value.FromValue.Null, Value.update(40))
//      cache.flattenClear.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.update(40))
//      cache.flattenClear.get(15: Slice[Byte]).getS shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
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
//      val cache = LevelZeroMapCache.builder.create()
//
//      //insert with put
//      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.put(10), Value.update(20))))
//      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 15, Value.FromValue.Null, Value.update(40))))
//      cache.flattenClear should have size 3
//      val skipListArray = cache.flattenClear.toIterable.toArray
//
//      skipListArray(0) shouldBe(1: Slice[Byte], Memory.Range(1, 10, Value.FromValue.Null, Value.update(40)))
//      skipListArray(1) shouldBe(10: Slice[Byte], Memory.Range(10, 15, Value.put(40), Value.update(40)))
//      skipListArray(2) shouldBe(15: Slice[Byte], Memory.Range(15, 20, Value.FromValue.Null, Value.update(20)))
//    }
//
//    "insert overlapping ranges when insert fromKey is greater than existing range's fromKey" in {
//      val cache = LevelZeroMapCache.builder.create()
//      //10
//      //1
//      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 15, Value.FromValue.Null, Value.update(40))))
//      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20))))
//      cache.flattenClear should have size 3
//
//      cache.flattenClear.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 10, Value.FromValue.Null, Value.update(40))
//      cache.flattenClear.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.update(20))
//      cache.flattenClear.get(15: Slice[Byte]).getS shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
//    }
//
//    "insert overlapping ranges when insert fromKey is greater than existing range's fromKey and fromKey is set" in {
//      val cache = LevelZeroMapCache.builder.create()
//      //15
//      //1 (Put(1))
//      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 15, Value.put(1), Value.update(40))))
//      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20))))
//
//      cache.flattenClear should have size 3
//
//      cache.flattenClear.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 10, Value.put(1), Value.update(40))
//      cache.flattenClear.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.update(20))
//      cache.flattenClear.get(15: Slice[Byte]).getS shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
//    }
//
//    "insert overlapping ranges without values set and no splits required" in {
//      val cache = LevelZeroMapCache.builder.create()
//      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 5, Value.FromValue.Null, Value.update(5))))
//      cache.writeNonAtomic(MapEntry.Put(5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))))
//      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20))))
//      cache.writeNonAtomic(MapEntry.Put(20, Memory.Range(20, 30, Value.FromValue.Null, Value.update(30))))
//      cache.writeNonAtomic(MapEntry.Put(30, Memory.Range(30, 40, Value.FromValue.Null, Value.update(40))))
//      cache.writeNonAtomic(MapEntry.Put(40, Memory.Range(40, 50, Value.FromValue.Null, Value.update(50))))
//
//      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 100, Value.FromValue.Null, Value.update(100))))
//      cache.flattenClear should have size 7
//
//      cache.flattenClear.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 5, Value.FromValue.Null, Value.update(5))
//      cache.flattenClear.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))
//      cache.flattenClear.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 20, Value.FromValue.Null, Value.update(100))
//      cache.flattenClear.get(20: Slice[Byte]).getS shouldBe Memory.Range(20, 30, Value.FromValue.Null, Value.update(100))
//      cache.flattenClear.get(30: Slice[Byte]).getS shouldBe Memory.Range(30, 40, Value.FromValue.Null, Value.update(100))
//      cache.flattenClear.get(40: Slice[Byte]).getS shouldBe Memory.Range(40, 50, Value.FromValue.Null, Value.update(100))
//      cache.flattenClear.get(50: Slice[Byte]).getS shouldBe Memory.Range(50, 100, Value.FromValue.Null, Value.update(100))
//    }
//
//    "insert overlapping ranges with values set and no splits required" in {
//      val cache = LevelZeroMapCache.builder.create()
//      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 5, Value.put(1), Value.update(5))))
//      cache.writeNonAtomic(MapEntry.Put(5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))))
//      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.put(10), Value.update(20))))
//      cache.writeNonAtomic(MapEntry.Put(20, Memory.Range(20, 30, Value.FromValue.Null, Value.update(30))))
//      cache.writeNonAtomic(MapEntry.Put(30, Memory.Range(30, 40, Value.put(30), Value.update(40))))
//      cache.writeNonAtomic(MapEntry.Put(40, Memory.Range(40, 50, Value.FromValue.Null, Value.update(50))))
//
//      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 100, Value.FromValue.Null, Value.update(100))))
//      cache.flattenClear should have size 7
//
//      cache.flattenClear.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 5, Value.put(1), Value.update(5))
//      cache.flattenClear.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))
//      cache.flattenClear.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 20, Value.put(100), Value.update(100))
//      cache.flattenClear.get(20: Slice[Byte]).getS shouldBe Memory.Range(20, 30, Value.FromValue.Null, Value.update(100))
//      cache.flattenClear.get(30: Slice[Byte]).getS shouldBe Memory.Range(30, 40, Value.put(100), Value.update(100))
//      cache.flattenClear.get(40: Slice[Byte]).getS shouldBe Memory.Range(40, 50, Value.FromValue.Null, Value.update(100))
//      cache.flattenClear.get(50: Slice[Byte]).getS shouldBe Memory.Range(50, 100, Value.FromValue.Null, Value.update(100))
//    }
//
//    "insert overlapping ranges with values set and splits required" in {
//      val cache = LevelZeroMapCache.builder.create()
//      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 5, Value.put(1), Value.update(5))))
//      cache.writeNonAtomic(MapEntry.Put(5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))))
//      cache.writeNonAtomic(MapEntry.Put(10, Memory.Range(10, 20, Value.put(10), Value.update(20))))
//      cache.writeNonAtomic(MapEntry.Put(20, Memory.Range(20, 30, Value.FromValue.Null, Value.update(30))))
//      cache.writeNonAtomic(MapEntry.Put(30, Memory.Range(30, 40, Value.put(30), Value.update(40))))
//      cache.writeNonAtomic(MapEntry.Put(40, Memory.Range(40, 50, Value.FromValue.Null, Value.update(50))))
//
//      cache.writeNonAtomic(MapEntry.Put(7, Memory.Range(7, 35, Value.FromValue.Null, Value.update(100))))
//      cache.flattenClear should have size 8
//
//      cache.flattenClear.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 5, Value.put(1), Value.update(5))
//      cache.flattenClear.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 7, Value.FromValue.Null, Value.update(10))
//      cache.flattenClear.get(7: Slice[Byte]).getS shouldBe Memory.Range(7, 10, Value.FromValue.Null, Value.update(100))
//      cache.flattenClear.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 20, Value.put(100), Value.update(100))
//      cache.flattenClear.get(20: Slice[Byte]).getS shouldBe Memory.Range(20, 30, Value.FromValue.Null, Value.update(100))
//      cache.flattenClear.get(30: Slice[Byte]).getS shouldBe Memory.Range(30, 35, Value.put(100), Value.update(100))
//      cache.flattenClear.get(35: Slice[Byte]).getS shouldBe Memory.Range(35, 40, Value.FromValue.Null, Value.update(40))
//      cache.flattenClear.get(40: Slice[Byte]).getS shouldBe Memory.Range(40, 50, Value.FromValue.Null, Value.update(50))
//    }
//
//    "remove range should remove invalid entries" in {
//      val cache = LevelZeroMapCache.builder.create()
//      cache.writeNonAtomic(MapEntry.Put(1, Memory.put(1, 1)))
//      cache.writeNonAtomic(MapEntry.Put(2, Memory.put(2, 2)))
//      cache.writeNonAtomic(MapEntry.Put(4, Memory.put(4, 4)))
//      cache.writeNonAtomic(MapEntry.Put(5, Memory.put(5, 5)))
//
//      cache.writeNonAtomic(MapEntry.Put(2, Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None))))
//
//      cache.flattenClear should have size 3
//
//      cache.flattenClear.get(1: Slice[Byte]).getS shouldBe Memory.put(1, 1)
//      cache.flattenClear.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None))
//      cache.flattenClear.get(3: Slice[Byte]).toOptionS shouldBe empty
//      cache.flattenClear.get(4: Slice[Byte]).toOptionS shouldBe empty
//      cache.flattenClear.get(5: Slice[Byte]).getS shouldBe Memory.put(5, 5)
//
//      cache.writeNonAtomic(MapEntry.Put(5, Memory.remove(5)))
//      cache.flattenClear.get(5: Slice[Byte]).getS shouldBe Memory.remove(5)
//
//      cache.flattenClear should have size 3
//    }
//
//    "remove range when cache.asMergedSkipList is empty" in {
//      val cache = LevelZeroMapCache.builder.create()
//      cache.writeNonAtomic(MapEntry.Put(2, Memory.Range(2, 100, Value.FromValue.Null, Value.remove(None))))
//      cache.flattenClear should have size 1
//
//      cache.flattenClear.get(1: Slice[Byte]).toOptionS shouldBe empty
//      cache.flattenClear.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 100, Value.FromValue.Null, Value.remove(None))
//      cache.flattenClear.get(3: Slice[Byte]).toOptionS shouldBe empty
//      cache.flattenClear.get(4: Slice[Byte]).toOptionS shouldBe empty
//    }
//
//    "remove range should clear removed entries when remove ranges overlaps the left edge" in {
//      val cache = LevelZeroMapCache.builder.create()
//      //1           -              10
//      (1 to 10) foreach {
//        i =>
//          cache.writeNonAtomic(MapEntry.Put(i, Memory.put(i, i)))
//      }
//
//      //1           -              10
//      //       4    -      8
//      cache.writeNonAtomic(MapEntry.Put(4, Memory.Range(4, 8, Value.FromValue.Null, Value.remove(None))))
//      cache.writeNonAtomic(MapEntry.Put(8, Memory.remove(8)))
//      //1           -              10
//      //   2    -    5
//      cache.writeNonAtomic(MapEntry.Put(2, Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None))))
//
//      cache.flattenClear.get(1: Slice[Byte]).getS shouldBe Memory.put(1, 1)
//      cache.flattenClear.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 4, Value.FromValue.Null, Value.remove(None))
//      cache.flattenClear.get(3: Slice[Byte]).toOptionS shouldBe empty
//      cache.flattenClear.get(4: Slice[Byte]).getS shouldBe Memory.Range(4, 5, Value.FromValue.Null, Value.remove(None))
//      cache.flattenClear.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 8, Value.FromValue.Null, Value.remove(None))
//      cache.flattenClear.get(6: Slice[Byte]).toOptionS shouldBe empty
//      cache.flattenClear.get(7: Slice[Byte]).toOptionS shouldBe empty
//      cache.flattenClear.get(8: Slice[Byte]).getS shouldBe Memory.remove(8)
//      cache.flattenClear.get(9: Slice[Byte]).getS shouldBe Memory.put(9, 9)
//      cache.flattenClear.get(10: Slice[Byte]).getS shouldBe Memory.put(10, 10)
//    }
//
//    "remove range should clear removed entries when remove ranges overlaps the right edge" in {
//      val cache = LevelZeroMapCache.builder.create()
//      //1           -              10
//      (1 to 10) foreach {
//        i =>
//          cache.writeNonAtomic(MapEntry.Put(i, Memory.put(i, i)))
//      }
//      //1           -              10
//      //   2    -    5
//      cache.writeNonAtomic(MapEntry.Put(2, Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None))))
//      cache.writeNonAtomic(MapEntry.Put(5, Memory.remove(5)))
//      //1           -              10
//      //       4    -      8
//      cache.writeNonAtomic(MapEntry.Put(4, Memory.Range(4, 8, Value.FromValue.Null, Value.remove(None))))
//      //      cache.write(MapEntry.Put(8, Memory.remove(8)))
//
//      cache.flattenClear.get(1: Slice[Byte]).getS shouldBe Memory.put(1, 1)
//      cache.flattenClear.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 4, Value.FromValue.Null, Value.remove(None))
//      cache.flattenClear.get(3: Slice[Byte]).toOptionS shouldBe empty
//      cache.flattenClear.get(4: Slice[Byte]).getS shouldBe Memory.Range(4, 5, Value.FromValue.Null, Value.remove(None))
//      cache.flattenClear.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 8, Value.FromValue.Null, Value.remove(None))
//      cache.flattenClear.get(6: Slice[Byte]).toOptionS shouldBe empty
//      cache.flattenClear.get(7: Slice[Byte]).toOptionS shouldBe empty
//      cache.flattenClear.get(8: Slice[Byte]).getS shouldBe Memory.put(8, 8)
//      cache.flattenClear.get(9: Slice[Byte]).getS shouldBe Memory.put(9, 9)
//      cache.flattenClear.get(10: Slice[Byte]).getS shouldBe Memory.put(10, 10)
//    }
//
//    "insert fixed key-values into remove range" in {
//      val cache = LevelZeroMapCache.builder.create()
//      //1           -              10
//      cache.writeNonAtomic(MapEntry.Put(1, Memory.Range(1, 10, Value.FromValue.Null, Value.remove(None))))
//      (1 to 10) foreach {
//        i =>
//          cache.writeNonAtomic(MapEntry.Put(i, Memory.put(i, i)))
//      }
//
//      cache.flattenClear.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 2, Value.put(1), Value.remove(None))
//      cache.flattenClear.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 3, Value.put(2), Value.remove(None))
//      cache.flattenClear.get(3: Slice[Byte]).getS shouldBe Memory.Range(3, 4, Value.put(3), Value.remove(None))
//      cache.flattenClear.get(4: Slice[Byte]).getS shouldBe Memory.Range(4, 5, Value.put(4), Value.remove(None))
//      cache.flattenClear.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 6, Value.put(5), Value.remove(None))
//      cache.flattenClear.get(6: Slice[Byte]).getS shouldBe Memory.Range(6, 7, Value.put(6), Value.remove(None))
//      cache.flattenClear.get(7: Slice[Byte]).getS shouldBe Memory.Range(7, 8, Value.put(7), Value.remove(None))
//      cache.flattenClear.get(8: Slice[Byte]).getS shouldBe Memory.Range(8, 9, Value.put(8), Value.remove(None))
//      cache.flattenClear.get(9: Slice[Byte]).getS shouldBe Memory.Range(9, 10, Value.put(9), Value.remove(None))
//      cache.flattenClear.get(10: Slice[Byte]).getS shouldBe Memory.put(10, 10)
//    }
//  }
//
//  "merge last two levels" when {
//    "limit is reached" in {
//      runThis(100.times, log = true) {
//        implicit val optimiseWrites: OptimiseWrites = OptimiseWrites.random(3)
//
//        optimiseWrites.transactionQueueMaxSize shouldBe 3
//
//        val cache = LevelZeroMapCache.builder.create()
//
//        cache.writeAtomic(MapEntry.Put(1, Memory.Put(1, "old", None, Time.empty)))
//        cache.writeAtomic(MapEntry.Put(1, Memory.Range(1, 100, FromValue.Null, Value.Update("update1", None, Time.empty))))
//        //outcome is still one since put and range have the same key
//        cache.levels.size shouldBe 1
//
//        cache.writeAtomic(MapEntry.Put(1, Memory.Range(50, 100, FromValue.Null, Value.Update("update2", None, Time.empty))))
//        //no collapse occurs
//        cache.levels.size shouldBe 2
//
//        cache.writeAtomic(MapEntry.Put(1, Memory.Range(75, 100, FromValue.Null, Value.Update("update3", None, Time.empty))))
//        //collapse occurs
//        cache.levels.size shouldBe 2
//
//        //newest
//        cache.levels.headOrNull().skipList.values().size shouldBe 1
//        cache.levels.headOrNull().skipList.values().toList.last shouldBe Memory.Range(75, 100, FromValue.Null, Value.Update("update3", None, Time.empty))
//
//        cache.levels.lastOrNull().skipList.values().size shouldBe 2
//        cache.levels.lastOrNull().skipList.values().toList shouldBe
//          List(
//            Memory.Range(1, 50, Value.Put("update1", None, Time.empty), Value.Update("update1", None, Time.empty)),
//            Memory.Range(50, 100, FromValue.Null, Value.Update("update2", None, Time.empty))
//          )
//      }
//    }
//  }
//}
