/////*
//// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
//// *
//// * Licensed under the Apache License, Version 2.0 (the "License");
//// * you may not use this file except in compliance with the License.
//// * You may obtain a copy of the License at
//// *
//// * http://www.apache.org/licenses/LICENSE-2.0
//// *
//// * Unless required by applicable law or agreed to in writing, software
//// * distributed under the License is distributed on an "AS IS" BASIS,
//// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// * See the License for the specific language governing permissions and
//// * limitations under the License.
//// */
////
////package swaydb.core.level
////
////import org.scalamock.scalatest.MockFactory
////import swaydb.effect.IOValues._
////import swaydb.config.MMAP
////import swaydb.config.compaction.LevelThrottle
////import swaydb.core.CommonAssertions._
////import swaydb.core.CoreTestData._
////import swaydb.core._
////import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
////import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
////import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
////import swaydb.core.segment.block.segment.SegmentBlockConfig
////import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
////import swaydb.core.segment.block.values.ValuesBlockConfig
////import swaydb.core.segment.io.SegmentReadIO
////import swaydb.core.segment.ref.search.ThreadReadState
////import swaydb.effect.Effect._
////import swaydb.serializers.Default._
////import swaydb.serializers._
////import swaydb.slice.Slice
////import swaydb.slice.order.{KeyOrder, TimeOrder}
////import swaydb.utils.OperatingSystem
////import swaydb.utils.StorageUnits._
////
////import scala.concurrent.duration._
////import swaydb.testkit.TestKit._
////
////class LevelReadSpec0 extends LevelReadSpec
////
////class LevelReadSpec1 extends LevelReadSpec {
////  override def levelFoldersCount = 10
////  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = GenForceSave.mmap())
////  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = GenForceSave.mmap())
////  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = GenForceSave.mmap())
////}
////
////class LevelReadSpec2 extends LevelReadSpec {
////  override def levelFoldersCount = 10
////  override def mmapSegments = MMAP.Off(forceSave = GenForceSave.standard())
////  override def level0MMAP = MMAP.Off(forceSave = GenForceSave.standard())
////  override def appendixStorageMMAP = MMAP.Off(forceSave = GenForceSave.standard())
////}
////
////class LevelReadSpec3 extends LevelReadSpec {
////  override def isMemorySpec = true
////}
////
////sealed trait LevelReadSpec extends AnyWordSpec {
////
////  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
////  implicit def testTimer: TestTimer = TestTimer.Empty
////  implicit val ec = TestExecutionContext.executionContext
////  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
////  implicit def segmentIO: SegmentReadIO = SegmentReadIO.random
////
////  val keyValuesCount = 100
////
////  "Level.mightContainKey" should {
////    "return true for key-values that exists or else false (bloom filter test on reboot)" in {
////      CoreTestSweeper {
////        implicit sweeper =>
////          import sweeper._
////
////          val keyValues = randomPutKeyValues(keyValuesCount, addPutDeadlines = false)
////
////          def assert(level: Level) = {
////            keyValues foreach {
////              keyValue =>
////                level.mightContainKey(keyValue.key, ThreadReadState.random).runRandomIO.get shouldBe true
////            }
////
////            level.mightContainKey("THIS KEY DOES NOT EXISTS", ThreadReadState.random).runRandomIO.get shouldBe false
////          }
////
////          val level = TestLevel(bloomFilterConfig = BloomFilterBlockConfig.random.copy(falsePositiveRate = 0.01))
////          level.put(keyValues).runRandomIO.get
////
////          assert(level)
////          if (isPersistent) assert(level.reopen)
////      }
////    }
////  }
////
////  "Level.takeSmallSegments" should {
////    "filter smaller segments from a Level" in {
////      CoreTestSweeper {
////        implicit sweeper =>
////          import sweeper._
////
////          //disable throttling so small segment compaction does not occur
////          val level = TestLevel(nextLevel = None, throttle = (_) => LevelThrottle(Duration.Zero, 0), segmentConfig = SegmentBlockConfig.random2(minSegmentSize = 1.kb))
////
////          val keyValues = randomPutKeyValues(1000, addPutDeadlines = false)
////          level.put(keyValues).runRandomIO.get
////          //do another put so split occurs.
////          level.put(keyValues.headSlice).runRandomIO.get
////          level.segmentsCount() > 1 shouldBe true //ensure there are Segments in this Level
////
////          if (isPersistent) {
////            val reopen = level.reopen(segmentSize = 10.mb)
////
////            reopen.takeSmallSegments(10000) should not be empty
////            //iterate again on the same Iterable.
////            // This test is to ensure that returned List is not a java Iterable which is only iterable once.
////            reopen.takeSmallSegments(10000) should not be empty
////
////            reopen.reopen(segmentSize = 10.mb).takeLargeSegments(1).isEmpty shouldBe true
////          }
////      }
////    }
////  }
////
////  "Level.meter" should {
////    "return Level stats" in {
////      CoreTestSweeper {
////        implicit sweeper =>
////          import sweeper._
////
////          val level = TestLevel()
////
////          val putKeyValues = randomPutKeyValues(keyValuesCount)
////          //refresh so that if there is a compression running, this Segment will compressed.
////          val segments =
////            GenSegment(putKeyValues, segmentConfig = SegmentBlockConfig.random(minSegmentSize = Int.MaxValue, maxKeyValuesPerSegment = Int.MaxValue, mmap = mmapSegments))
////              .runRandomIO
////              .right.value
////              .refresh(
////                removeDeletes = false,
////                createdInLevel = 0,
////                valuesConfig = ValuesBlockConfig.random,
////                sortedIndexConfig = SortedIndexBlockConfig.random,
////                binarySearchIndexConfig = BinarySearchIndexBlockConfig.random,
////                hashIndexConfig = HashIndexBlockConfig.random,
////                bloomFilterConfig = BloomFilterBlockConfig.random,
////                segmentConfig = SegmentBlockConfig.random2(minSegmentSize = 100.mb),
////                pathDistributor = createPathDistributor()
////              ).runRandomIO.get
////
////          segments should have size 1
////          val segment = segments.head
////
////          level.put(segment).get
////
////          level.meter.segmentsCount shouldBe 1
////          level.meter.levelSize shouldBe segment.segmentSize
////      }
////    }
////  }
////
////  "Level.meterFor" should {
////    "forward request to the right level" in {
////      CoreTestSweeper {
////        implicit sweeper =>
////          import sweeper._
////
////          val level2 = TestLevel()
////          val level1 = TestLevel(nextLevel = Some(level2))
////
////          val putKeyValues = randomPutKeyValues(keyValuesCount)
////          //refresh so that if there is a compression running, this Segment will compressed.
////          val segments =
////            GenSegment(putKeyValues, segmentConfig = SegmentBlockConfig.random(minSegmentSize = Int.MaxValue, maxKeyValuesPerSegment = Int.MaxValue, mmap = mmapSegments))
////              .runRandomIO.get
////              .refresh(
////                removeDeletes = false,
////                createdInLevel = 0,
////                valuesConfig = ValuesBlockConfig.random,
////                sortedIndexConfig = SortedIndexBlockConfig.random,
////                binarySearchIndexConfig = BinarySearchIndexBlockConfig.random,
////                hashIndexConfig = HashIndexBlockConfig.random,
////                bloomFilterConfig = BloomFilterBlockConfig.random,
////                segmentConfig = SegmentBlockConfig.random2(minSegmentSize = 100.mb),
////                pathDistributor = createPathDistributor()
////              ).runRandomIO.get
////
////          segments should have size 1
////          val segment = segments.head
////
////          level2.put(segment).get
////
////          level1.meter.levelSize shouldBe 0
////          level1.meter.segmentsCount shouldBe 0
////
////          val level1Meter = level1.meterFor(level1.pathDistributor.headPath.folderId.toInt).get
////          level1Meter.levelSize shouldBe 0
////          level1Meter.segmentsCount shouldBe 0
////
////          level2.meter.segmentsCount shouldBe 1
////          level2.meter.levelSize shouldBe segment.segmentSize
////
////          val level2Meter = level1.meterFor(level2.pathDistributor.headPath.folderId.toInt).get
////          level2Meter.segmentsCount shouldBe 1
////          level2Meter.levelSize shouldBe segment.segmentSize
////      }
////    }
////
////    "return None is Level does not exist" in {
////      CoreTestSweeper {
////        implicit sweeper =>
////          import sweeper._
////
////          val level2 = TestLevel()
////          val level1 = TestLevel(nextLevel = Some(level2))
////
////          val putKeyValues = randomPutKeyValues(keyValuesCount)
////          val segment = GenSegment(putKeyValues).runRandomIO.get
////          level2.put(segment).get
////
////          level1.meterFor(3) shouldBe empty
////      }
////    }
////  }
////}
