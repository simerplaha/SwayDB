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
//
//package swaydb.core.segment
//
//import java.nio.file.NoSuchFileException
//
//import org.scalamock.scalatest.MockFactory
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestCaseSweeper._
//import swaydb.core.TestData._
//import swaydb.core.data.{Memory, Persistent}
//import swaydb.core.io.file.Effect
//import swaydb.core.segment.assigner.Assignable
//import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
//import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
//import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
//import swaydb.core.segment.format.a.block.segment.SegmentBlock
//import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
//import swaydb.core.segment.format.a.block.values.ValuesBlock
//import swaydb.core.{TestBase, TestCaseSweeper, TestData, TestExecutionContext, TestTimer}
//import swaydb.data.RunThis._
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//import swaydb.data.util.StorageUnits._
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.{Error, IO}
//
//class SegmentRefSpec extends TestBase with MockFactory {
//
//  implicit val keyOrder = KeyOrder.default
//  implicit val testTimer: TestTimer = TestTimer.Empty
//  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))
//  implicit val persistentKeyOrder: KeyOrder[Persistent] = KeyOrder(Ordering.by[Persistent, Slice[Byte]](_.key)(keyOrder))
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//  implicit def segmentIO = SegmentReadIO.random
//  implicit val ec = TestExecutionContext.executionContext
//
//  "fastPutMany" should {
//    "succeed" in {
//      runThis(10.times, log = true) {
//        TestCaseSweeper {
//          implicit sweeper =>
//
//            val segmentKeyValues: Slice[Memory] = randomKeyValues(100)
//
//            val segment: Segment = TestSegment(segmentKeyValues)
//
//            val maxKey = segment.maxKey.maxKey.readInt[Byte]()
//            val otherKeyValues: Slice[Memory] = randomKeyValues(startId = Some(maxKey), count = 1000)
//
//            val assignables: Iterable[Iterable[Assignable]] = Seq(Seq(segment), otherKeyValues)
//
//            val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
//            val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
//            val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
//            val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
//            val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
//            val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random(mmap = mmapSegments)
//            val pathDistributor = createPathDistributor
//
//            val putResult =
//              SegmentRef.fastPutMany(
//                assignables = assignables,
//                segmentParallelism = randomParallelMerge().segment,
//                removeDeletes = false,
//                createdInLevel = 0,
//                valuesConfig = valuesConfig,
//                sortedIndexConfig = sortedIndexConfig,
//                binarySearchIndexConfig = binarySearchIndexConfig,
//                hashIndexConfig = hashIndexConfig,
//                bloomFilterConfig = bloomFilterConfig,
//                segmentConfig = segmentConfig
//              ).get.map(_.persist(pathDistributor).get)
//
//            val allKeyValues = segmentKeyValues ++ otherKeyValues
//
//            //since there is one segment in the assignable that segment is always copied so
//            val totalSegments = putResult.foldLeft(0)(_ + _.size)
//            totalSegments should be >= 2
//            putResult.head should have size 1
//            putResult.head.head.iterator().toList shouldBe segmentKeyValues
//
//            putResult.foreach(_.foreach(_.sweep()))
//            putResult.flatMap(_.flatMap(_.iterator())) shouldBe allKeyValues
//        }
//      }
//    }
//
//    //    "fail" in {
//    //      runThis(10.times, log = true) {
//    //        TestCaseSweeper {
//    //          implicit sweeper =>
//    //            import sweeper._
//    //
//    //            val segmentKeyValues1: Slice[Memory] = randomKeyValues(100)
//    //            val segment1: Segment = TestSegment(segmentKeyValues1)
//    //            val maxKey1 = segment1.maxKey.maxKey.readInt[Byte]()
//    //
//    //            val segmentKeyValues2: Slice[Memory] = randomKeyValues(100)
//    //            val segment2: Segment = TestSegment(segmentKeyValues2)
//    //            segment2.delete
//    //
//    //            val otherKeyValues: Slice[Memory] = randomKeyValues(startId = Some(maxKey1), count = 1000)
//    //
//    //            val assignables: Iterable[Iterable[Assignable]] =
//    //              eitherOne(
//    //                Seq(Seq(segment1, segment2), otherKeyValues),
//    //                Seq(Seq(segment1), Seq(segment2), otherKeyValues),
//    //                Seq(Seq(segment1), otherKeyValues ++ Slice(segment2))
//    //              )
//    //
//    //            val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
//    //            val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
//    //            val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
//    //            val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
//    //            val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
//    //            val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random(mmap = mmapSegments)
//    //            val pathDistributor = createPathDistributor.sweep(_.headPath.sweep())
//    //
//    //            val putResult: IO[Error.Segment, Iterable[Slice[PersistentSegment]]] =
//    //              SegmentRef.fastPutMany(
//    //                assignables = assignables,
//    //                segmentParallelism = randomParallelMerge().segment,
//    //                removeDeletes = false,
//    //                createdInLevel = 0,
//    //                valuesConfig = valuesConfig,
//    //                sortedIndexConfig = sortedIndexConfig,
//    //                binarySearchIndexConfig = binarySearchIndexConfig,
//    //                hashIndexConfig = hashIndexConfig,
//    //                bloomFilterConfig = bloomFilterConfig,
//    //                segmentConfig = segmentConfig,
//    //                pathsDistributor = pathDistributor
//    //              )
//    //
//    //            putResult.left.get.exception shouldBe a[NoSuchFileException]
//    //
//    //            Effect.isEmptyOrNotExists(pathDistributor.headPath).get shouldBe true
//    //        }
//    //      }
//    //    }
//  }
//
//  //  "fastAssignPut" when {
//  //    "basic setting" in {
//  //      runThis(10.times, log = true) {
//  //        TestCaseSweeper {
//  //          implicit sweeper =>
//  //            import sweeper._
//  //
//  //            //create a many segment with SegmentRef
//  //            val segmentKeyValues: Slice[Memory] = randomKeyValues(100, startId = Some(100))
//  //
//  //            //large segmentSize so that single segment with multiple SegmentRefs is created
//  //            val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random(minSegmentSize = 4.mb, maxKeyValuesPerSegment = segmentKeyValues.size / 2)
//  //            implicit val pathDistributor = createPathDistributor.sweep(_.headPath.sweep())
//  //
//  //            val segment = TestSegment.many(keyValues = segmentKeyValues, segmentConfig = segmentConfig)
//  //
//  //            segment should have size 1
//  //            segment.head.isInstanceOf[PersistentSegmentMany] shouldBe true
//  //
//  //            val manySegment = segment.head.asInstanceOf[PersistentSegmentMany]
//  //            manySegment.getAllSegmentRefs() should have size 2
//  //
//  //            //fetch the SegmentRefs
//  //            val segmentRef1 = manySegment.getAllSegmentRefs().toList.head
//  //            val segmentRef2 = manySegment.getAllSegmentRefs().toList.last
//  //
//  //            //provide gap key-values
//  //            val segmentRef2MaxKey = segmentRef2.maxKey.maxKey.readInt()
//  //            val headGap = Slice(Memory.put(1, 1))
//  //            val tailGap = Slice(Memory.put(segmentRef2MaxKey + 1, segmentRef2MaxKey + 1))
//  //
//  //            val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
//  //            val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
//  //            val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
//  //            val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
//  //            val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
//  //
//  //            /**
//  //             * WITH GAP
//  //             */
//  //            {
//  //              val putResult =
//  //                SegmentRef.fastAssignPut(
//  //                  headGap = headGap,
//  //                  tailGap = tailGap,
//  //                  segmentRefs = Iterator(segmentRef1, segmentRef2),
//  //                  assignableCount = segmentKeyValues.size,
//  //                  assignables = segmentKeyValues.iterator,
//  //                  removeDeletes = false,
//  //                  createdInLevel = 1,
//  //                  segmentParallelism = randomParallelMerge().segment,
//  //                  valuesConfig = valuesConfig,
//  //                  sortedIndexConfig = sortedIndexConfig,
//  //                  binarySearchIndexConfig = binarySearchIndexConfig,
//  //                  hashIndexConfig = hashIndexConfig,
//  //                  bloomFilterConfig = bloomFilterConfig,
//  //                  segmentConfig = segmentConfig,
//  //                  pathsDistributor = pathDistributor
//  //                )
//  //
//  //              putResult.replaced shouldBe true
//  //              val allKeyValues = headGap ++ segmentKeyValues ++ tailGap
//  //              putResult.result.flatMap(_.iterator()).toList shouldBe allKeyValues
//  //            }
//  //
//  //            /**
//  //             * WITHOUT GAP
//  //             */
//  //            {
//  //              val putResult =
//  //                SegmentRef.fastAssignPut(
//  //                  headGap = Iterable.empty,
//  //                  tailGap = Iterable.empty,
//  //                  segmentRefs = Iterator(segmentRef1, segmentRef2),
//  //                  assignableCount = segmentKeyValues.size,
//  //                  assignables = segmentKeyValues.iterator,
//  //                  removeDeletes = false,
//  //                  createdInLevel = 1,
//  //                  segmentParallelism = randomParallelMerge().segment,
//  //                  valuesConfig = valuesConfig,
//  //                  sortedIndexConfig = sortedIndexConfig,
//  //                  binarySearchIndexConfig = binarySearchIndexConfig,
//  //                  hashIndexConfig = hashIndexConfig,
//  //                  bloomFilterConfig = bloomFilterConfig,
//  //                  segmentConfig = segmentConfig,
//  //                  pathsDistributor = pathDistributor
//  //                )
//  //
//  //              putResult.result should have size 1
//  //              putResult.replaced shouldBe true
//  //              putResult.result.flatMap(_.iterator()).toList shouldBe segmentKeyValues
//  //            }
//  //
//  //            /**
//  //             * WITHOUT MID
//  //             */
//  //            {
//  //              val putResult =
//  //                SegmentRef.fastAssignPut(
//  //                  headGap = headGap,
//  //                  tailGap = tailGap,
//  //                  segmentRefs = Iterator(segmentRef1, segmentRef2),
//  //                  assignableCount = 0,
//  //                  assignables = Iterator.empty,
//  //                  removeDeletes = false,
//  //                  createdInLevel = 1,
//  //                  segmentParallelism = randomParallelMerge().segment,
//  //                  valuesConfig = valuesConfig,
//  //                  sortedIndexConfig = sortedIndexConfig,
//  //                  binarySearchIndexConfig = binarySearchIndexConfig,
//  //                  hashIndexConfig = hashIndexConfig,
//  //                  bloomFilterConfig = bloomFilterConfig,
//  //                  segmentConfig = segmentConfig,
//  //                  pathsDistributor = pathDistributor
//  //                )
//  //
//  //              putResult.result should have size 2
//  //              putResult.replaced shouldBe false
//  //              putResult.result.flatMap(_.iterator()).toList shouldBe (headGap ++ tailGap)
//  //            }
//  //        }
//  //      }
//  //    }
//  //
//  //    "there are many SegmentRef that are randomly updated" should {
//  //      "succeed" in {
//  //        runThis(10.times, log = true) {
//  //          TestCaseSweeper {
//  //            implicit sweeper =>
//  //              import sweeper._
//  //
//  //              //create a many segment with SegmentRef
//  //              val segmentKeyValues: Slice[Memory] = randomKeyValues(10000, startId = Some(100))
//  //
//  //              //large segmentSize so that single segment with multiple SegmentRefs is created. Create many SegmentRefs
//  //              val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random(minSegmentSize = 10.mb, maxKeyValuesPerSegment = segmentKeyValues.size / 10)
//  //              implicit val pathDistributor = createPathDistributor.sweep(_.headPath.sweep())
//  //
//  //              val segment = TestSegment.many(keyValues = segmentKeyValues, segmentConfig = segmentConfig)
//  //
//  //              segment should have size 1
//  //              segment.head.isInstanceOf[PersistentSegmentMany] shouldBe true
//  //
//  //              val manySegment = segment.head.asInstanceOf[PersistentSegmentMany]
//  //              val segmentRefs = manySegment.getAllSegmentRefs().toArray
//  //              segmentRefs should have size 10
//  //
//  //              //random select SegmentRefs to update. SegmentRefs that are no updated should still exists.
//  //              val segment1KeyValue = Memory.put(segmentRefs(1).minKey, "updated")
//  //              val segment3KeyValue = Memory.put(segmentRefs(3).minKey, "updated")
//  //              val segment4KeyValue = Memory.put(segmentRefs(4).minKey, "updated")
//  //              val segment8KeyValue = Memory.put(segmentRefs(8).minKey, "updated")
//  //              val updatedKeyValues = Slice(segment1KeyValue, segment3KeyValue, segment4KeyValue, segment8KeyValue)
//  //
//  //              //expected merge result.
//  //              val expectedOutcome = TestData.merge(newKeyValues = updatedKeyValues, oldKeyValues = segmentKeyValues, isLastLevel = false)
//  //
//  //              //provide gap key-values
//  //              val segmentRef2MaxKey = segmentRefs.last.maxKey.maxKey.readInt()
//  //              val headGap = Slice(Memory.put(1, 1))
//  //              val tailGap = Slice(Memory.put(segmentRef2MaxKey + 1, segmentRef2MaxKey + 1))
//  //
//  //              val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
//  //              val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
//  //              val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
//  //              val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
//  //              val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
//  //
//  //              /**
//  //               * WITH GAP
//  //               */
//  //              {
//  //                val putResult =
//  //                  SegmentRef.fastAssignPut(
//  //                    headGap = headGap,
//  //                    tailGap = tailGap,
//  //                    segmentRefs = segmentRefs.iterator,
//  //                    assignableCount = updatedKeyValues.size,
//  //                    assignables = updatedKeyValues.iterator,
//  //                    removeDeletes = false,
//  //                    createdInLevel = 1,
//  //                    segmentParallelism = randomParallelMerge().segment,
//  //                    valuesConfig = valuesConfig,
//  //                    sortedIndexConfig = sortedIndexConfig,
//  //                    binarySearchIndexConfig = binarySearchIndexConfig,
//  //                    hashIndexConfig = hashIndexConfig,
//  //                    bloomFilterConfig = bloomFilterConfig,
//  //                    segmentConfig = segmentConfig,
//  //                    pathsDistributor = pathDistributor
//  //                  )
//  //
//  //                putResult.replaced shouldBe true
//  //                val allKeyValues = headGap ++ expectedOutcome ++ tailGap
//  //                putResult.result.flatMap(_.iterator()).toList shouldBe allKeyValues
//  //              }
//  //
//  //              /**
//  //               * WITHOUT GAP
//  //               */
//  //              {
//  //                val putResult =
//  //                  SegmentRef.fastAssignPut(
//  //                    headGap = Iterable.empty,
//  //                    tailGap = Iterable.empty,
//  //                    segmentRefs = segmentRefs.iterator,
//  //                    assignableCount = updatedKeyValues.size,
//  //                    assignables = updatedKeyValues.iterator,
//  //                    removeDeletes = false,
//  //                    createdInLevel = 1,
//  //                    segmentParallelism = randomParallelMerge().segment,
//  //                    valuesConfig = valuesConfig,
//  //                    sortedIndexConfig = sortedIndexConfig,
//  //                    binarySearchIndexConfig = binarySearchIndexConfig,
//  //                    hashIndexConfig = hashIndexConfig,
//  //                    bloomFilterConfig = bloomFilterConfig,
//  //                    segmentConfig = segmentConfig,
//  //                    pathsDistributor = pathDistributor
//  //                  )
//  //
//  //                putResult.result should have size 1
//  //                putResult.replaced shouldBe true
//  //                putResult.result.flatMap(_.iterator()).toList shouldBe expectedOutcome
//  //              }
//  //
//  //              /**
//  //               * WITHOUT MID
//  //               */
//  //              {
//  //                val putResult =
//  //                  SegmentRef.fastAssignPut(
//  //                    headGap = headGap,
//  //                    tailGap = tailGap,
//  //                    segmentRefs = segmentRefs.iterator,
//  //                    assignableCount = 0,
//  //                    assignables = Iterator.empty,
//  //                    removeDeletes = false,
//  //                    createdInLevel = 1,
//  //                    segmentParallelism = randomParallelMerge().segment,
//  //                    valuesConfig = valuesConfig,
//  //                    sortedIndexConfig = sortedIndexConfig,
//  //                    binarySearchIndexConfig = binarySearchIndexConfig,
//  //                    hashIndexConfig = hashIndexConfig,
//  //                    bloomFilterConfig = bloomFilterConfig,
//  //                    segmentConfig = segmentConfig,
//  //                    pathsDistributor = pathDistributor
//  //                  )
//  //
//  //                putResult.result should have size 2
//  //                putResult.replaced shouldBe false
//  //                putResult.result.flatMap(_.iterator()).toList shouldBe (headGap ++ tailGap)
//  //              }
//  //          }
//  //        }
//  //      }
//  //    }
//  //  }
//
//}
