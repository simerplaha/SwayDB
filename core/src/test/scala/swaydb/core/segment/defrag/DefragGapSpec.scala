/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.defrag

import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.Memory
import swaydb.core.level.PathsDistributor
import swaydb.core.merge.stats.{MergeStats, MergeStatsCreator, MergeStatsSizeCalculator}
import swaydb.core.segment._
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestTimer}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._
import swaydb.utils.StorageUnits._

import scala.collection.mutable.ListBuffer

/**
 * Test setup for when input types are [[PersistentSegment]]
 */
class PersistentSegment_DefragGapSpec extends DefragGapSpec[PersistentSegment, PersistentSegmentOption, MergeStats.Persistent.Builder[Memory, ListBuffer]] {

  override def testSegment(keyValues: Slice[Memory])(implicit sweeper: TestCaseSweeper): PersistentSegment =
    TestSegment(keyValues).shouldBeInstanceOf[PersistentSegment]

  override def nullSegment: PersistentSegmentOption =
    PersistentSegment.Null

  override implicit def mergeStatsCreator: MergeStatsCreator[MergeStats.Persistent.Builder[Memory, ListBuffer]] =
    MergeStatsCreator.PersistentCreator

  override implicit def mergeStatsSizeCalculator(implicit sortedIndexConfig: SortedIndexBlock.Config): MergeStatsSizeCalculator[MergeStats.Persistent.Builder[Memory, ListBuffer]] =
    MergeStatsSizeCalculator.persistentSizeCalculator(sortedIndexConfig)
}

/**
 * Test setup for when input types are [[MemorySegment]]
 */
class MemorySegment_DefragGapSpec extends DefragGapSpec[MemorySegment, MemorySegmentOption, MergeStats.Memory.Builder[Memory, ListBuffer]] {

  override def inMemoryStorage = true

  override def testSegment(keyValues: Slice[Memory])(implicit sweeper: TestCaseSweeper): MemorySegment =
    TestSegment(keyValues).shouldBeInstanceOf[MemorySegment]

  override def nullSegment: MemorySegmentOption =
    MemorySegment.Null

  override implicit def mergeStatsCreator: MergeStatsCreator[MergeStats.Memory.Builder[Memory, ListBuffer]] =
    MergeStatsCreator.MemoryCreator

  override implicit def mergeStatsSizeCalculator(implicit sortedIndexConfig: SortedIndexBlock.Config): MergeStatsSizeCalculator[MergeStats.Memory.Builder[Memory, ListBuffer]] =
    MergeStatsSizeCalculator.MemoryCreator
}


sealed trait DefragGapSpec[SEG <: Segment, NULL_SEG >: SEG, S >: Null <: MergeStats.Segment[Memory, ListBuffer]] extends TestBase with MockFactory with EitherValues {

  implicit val ec = TestExecutionContext.executionContext
  implicit val timer = TestTimer.Empty


  def testSegment(keyValues: Slice[Memory] = randomizedKeyValues())(implicit sweeper: TestCaseSweeper): SEG
  def nullSegment: NULL_SEG
  implicit def mergeStatsCreator: MergeStatsCreator[S]
  implicit def mergeStatsSizeCalculator(implicit sortedIndexConfig: SortedIndexBlock.Config): MergeStatsSizeCalculator[S]

  "add Segments" when {
    "there is no head MergeStats and no next and removeDeletes is false" in {
      TestCaseSweeper {
        implicit sweeper =>

          val segments: ListBuffer[SEG] = ListBuffer.range(1, 5).map(_ => testSegment())

          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = segments.map(_.segmentSize).min)

          val resultBuffer =
            DefragGap.run[S](
              gap = segments,
              fragments = ListBuffer.empty,
              removeDeletes = false,
              createdInLevel = 1,
              hasNext = false
            )

          if (persistent) {
            val expected =
              segments map {
                case segment: PersistentSegment =>
                  TransientSegment.RemotePersistentSegment(segment = segment)
              }

            resultBuffer shouldBe expected
          } else {
            resultBuffer should have size 1
            resultBuffer.head.shouldBeInstanceOf[TransientSegment.Stats[S]].stats.keyValues shouldBe segments.flatMap(_.iterator(randomBoolean()))
          }
      }
    }

    "there is head MergeStats but it is greater than segmentConfig.minSize" in {
      TestCaseSweeper {
        implicit sweeper =>

          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          //small minSize so that the size of head key-values is always considered large.
          implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = 1.byte)

          val keyValues = randomKeyValues(10, startId = Some(0))

          val headMergeStats = mergeStatsCreator.create(false)
          headMergeStats.addAll(keyValues)

          val headKeyValues = TransientSegment.Stats(headMergeStats)
          val fragments = ListBuffer[TransientSegment.Fragment[S]](headKeyValues)

          val segments = ListBuffer.range(1, 5).map(index => testSegment(randomKeyValues(10, startId = Some(10 * index))))

          val resultBuffer =
            DefragGap.run[S](
              gap = segments,
              fragments = fragments.toList.to(ListBuffer),
              removeDeletes = false,
              createdInLevel = 1,
              hasNext = false
            )

          if (persistent) {
            val expectedTail =
              segments map {
                case segment: PersistentSegment =>
                  TransientSegment.RemotePersistentSegment(segment = segment)
              }

            //expect the key-values and segments to get added
            resultBuffer should have size (segments.size + 1)

            //contains head key-values
            resultBuffer.head shouldBe headKeyValues
            //contain all the Segments
            resultBuffer.drop(1) shouldBe expectedTail
          } else {
            resultBuffer should have size 1
            val expectedKeyValues = keyValues ++ segments.flatMap(_.iterator(randomBoolean()))
            resultBuffer.head.shouldBeInstanceOf[TransientSegment.Stats[S]].stats.keyValues shouldBe expectedKeyValues
          }
      }
    }

    "there is head MergeStats but it is smaller than segmentConfig.minSize" in {
      //in this test the second PersistentSegmentOne should get merged into head stats.
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            //each segment has non removable key-values
            val segments = ListBuffer.range(0, 5).map(_ => TestSegment.one(randomPutKeyValues(20)))
            segments should have size 5

            implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
            //set size to be small enough so that head segment gets merged.
            implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = segments.map(_.segmentSize).min, maxCount = Int.MaxValue)

            //head key-values are too small.
            val initialKeyValues = Slice(Memory.put(1))

            //enough key-values to make head stats to be small
            val mergeStatsCreated = mergeStatsCreator.create(false)
            mergeStatsCreated.addAll(initialKeyValues)
            mergeStatsCreated.keyValues.size shouldBe initialKeyValues.size

            //yes head key-values are small
            mergeStatsSizeCalculator.isStatsOrNullSmall(statsOrNull = mergeStatsCreated) shouldBe true

            val fragments = ListBuffer[TransientSegment.Fragment[S]](TransientSegment.Stats(mergeStatsCreated))

            //mutation occurs and head stats get head segment's key-values
            val resultFragments =
              DefragGap.run(
                gap = segments,
                fragments = fragments,
                removeDeletes = false,
                createdInLevel = 1,
                hasNext = false
              )

            if (persistent) { //headKeyValues are larger than initial
              mergeStatsCreated.keyValues.size should be > initialKeyValues.size

              //first segment gets merged into merge stats and other 4 remain intact.
              resultFragments should have size 5

              //first one is a stats
              resultFragments.head shouldBe a[TransientSegment.Stats[S]]

              //all the other ares Segments
              resultFragments.drop(1) foreach {
                segment =>
                  segment shouldBe a[TransientSegment.RemotePersistentSegment]
              }

              //collect all key-values from all resulting fragments
              val fragmentKeyValues =
                resultFragments flatMap {
                  case remote: TransientSegment.RemotePersistentSegment =>
                    remote.iterator(randomBoolean())

                  case remote: TransientSegment.RemoteRef =>
                    fail(s"Unexpected ${TransientSegment.RemoteRef.getClass.getSimpleName}")

                  case TransientSegment.Fence =>
                    fail(s"Unexpected ${TransientSegment.Fence.productPrefix}")

                  case TransientSegment.Stats(stats) =>
                    stats.keyValues
                } toList

              //collect all expected key-values
              val allKeyValues = (initialKeyValues ++ segments.flatMap(_.iterator(randomBoolean()))).toList

              fragmentKeyValues shouldBe allKeyValues
            } else {
              resultFragments should have size 1
              val expectedKeyValues = initialKeyValues ++ segments.flatMap(_.iterator(randomBoolean()))
              resultFragments.head.shouldBeInstanceOf[TransientSegment.Stats[S]].stats.keyValues shouldBe expectedKeyValues
            }
        }
      }
    }

    "there is head MergeStats but it is small" when {
      "PersistentSegmentMany is input" in {
        //in this test the second SegmentRef from the one PersistentSegmentMany instance should get merged into head stats.
        //and expect PersistentSegmentMany to expand and SegmentRefs to get appended.
        if (memory)
          cancel("TODO")
        else
          runThis(10.times, log = true) {
            TestCaseSweeper {
              implicit sweeper =>

                implicit val pathsDistributor: PathsDistributor = createPathDistributor

                //a single
                val manySegment = TestSegment.many(keyValues = randomPutKeyValues(100), segmentConfig = SegmentBlock.Config.random.copy(minSize = Int.MaxValue, maxCount = 5))
                manySegment should have size 1
                manySegment.head.isInstanceOf[PersistentSegmentMany] shouldBe true
                manySegment.head.asInstanceOf[PersistentSegmentMany].segmentRefs(randomBoolean()) should have size 20

                implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
                //set size to be small enough so that head segment gets merged.
                implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = manySegment.head.segmentSize, maxCount = 5)

                //head key-values are too small.
                val initialKeyValues = Slice(Memory.put(1))

                //enough key-values to make head stats to be small
                val headKeyValues = mergeStatsCreator.create(false)
                headKeyValues.addAll(initialKeyValues)
                headKeyValues.keyValues.size shouldBe initialKeyValues.size

                //yes head key-values are small
                mergeStatsSizeCalculator.isStatsOrNullSmall(statsOrNull = headKeyValues) shouldBe true

                val fragments = ListBuffer[TransientSegment.Fragment[S]](TransientSegment.Stats(headKeyValues))

                //mutation occurs and head stats get head segment's key-values
                val resultFragments =
                  DefragGap.run[S](
                    gap = manySegment,
                    fragments = fragments,
                    removeDeletes = false,
                    createdInLevel = 1,
                    hasNext = false
                  )

                //headKeyValues are larger than initial
                headKeyValues.keyValues.size should be > initialKeyValues.size

                //first segment gets merged into merge stats and other 4 remain intact.
                resultFragments should have size 20

                //first one is a stats
                resultFragments.head shouldBe a[TransientSegment.Stats[S]]

                //all the other ares Segments
                resultFragments.drop(1) foreach {
                  segment =>
                    segment shouldBe a[TransientSegment.RemoteRef]
                }

                //collect all key-values from all resulting fragments
                val fragmentKeyValues =
                  resultFragments flatMap {
                    case remote: TransientSegment.RemoteRef =>
                      remote.iterator(randomBoolean())

                    case remote: TransientSegment.RemotePersistentSegment =>
                      fail(s"Unexpected ${TransientSegment.RemotePersistentSegment.getClass.getSimpleName}")

                    case TransientSegment.Fence =>
                      fail(s"Unexpected ${TransientSegment.Fence.productPrefix}")

                    case TransientSegment.Stats(stats) =>
                      stats.keyValues
                  } toList

                //collect all expected key-values
                val allKeyValues = (initialKeyValues ++ manySegment.flatMap(_.iterator(randomBoolean()))).toList

                fragmentKeyValues shouldBe allKeyValues
            }
          }
      }

      "SegmentRefs are input" in {
        //in this test the second SegmentRef from the one PersistentSegmentMany instance should get merged into head stats.
        if (memory)
          cancel("TODO")
        else
          runThis(10.times, log = true) {
            TestCaseSweeper {
              implicit sweeper =>

                implicit val pathsDistributor = createPathDistributor

                val manySegment = TestSegment.many(keyValues = randomPutKeyValues(100), segmentConfig = SegmentBlock.Config.random.copy(minSize = Int.MaxValue, maxCount = 5))
                manySegment should have size 1
                manySegment.head.isInstanceOf[PersistentSegmentMany] shouldBe true

                val segmentRefs = manySegment.head.asInstanceOf[PersistentSegmentMany].segmentRefs(randomBoolean()).toList

                implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
                //set size to be small enough so that head segment gets merged.
                implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = Int.MaxValue, maxCount = 5)

                //head key-values are too small.
                val initialKeyValues = Slice(Memory.put(1))

                //enough key-values to make head stats to be small
                val headKeyValues = mergeStatsCreator.create(false)
                headKeyValues.addAll(initialKeyValues)
                headKeyValues.keyValues.size shouldBe initialKeyValues.size

                //yes head key-values are small
                mergeStatsSizeCalculator.isStatsOrNullSmall(statsOrNull = headKeyValues) shouldBe true

                val fragments = ListBuffer[TransientSegment.Fragment[S]](TransientSegment.Stats(headKeyValues))

                //mutation occurs and head stats get head segment's key-values
                val resultFragments =
                  DefragGap.run[S](
                    gap = segmentRefs,
                    fragments = fragments,
                    removeDeletes = false,
                    createdInLevel = 1,
                    hasNext = false
                  )

                //headKeyValues are larger than initial
                headKeyValues.result.size should be > initialKeyValues.size

                //first segment gets merged into merge stats and other 4 remain intact.
                resultFragments should have size 20

                //first one is a stats
                resultFragments.head shouldBe a[TransientSegment.Stats[S]]

                //all the other ares Segments
                resultFragments.drop(1) foreach {
                  segment =>
                    segment shouldBe a[TransientSegment.RemoteRef]
                }

                //collect all key-values from all resulting fragments
                val fragmentKeyValues =
                  resultFragments flatMap {
                    case remote: TransientSegment.RemoteRef =>
                      remote.iterator(randomBoolean())

                    case remote: TransientSegment.RemotePersistentSegment =>
                      fail(s"Unexpected ${TransientSegment.RemotePersistentSegment.getClass.getSimpleName}")

                    case TransientSegment.Fence =>
                      fail(s"Unexpected ${TransientSegment.Fence.productPrefix}")

                    case TransientSegment.Stats(stats) =>
                      stats.keyValues
                  } toList

                //collect all expected key-values
                val allKeyValues = (initialKeyValues ++ segmentRefs.flatMap(_.iterator(randomBoolean()))).toList

                fragmentKeyValues shouldBe allKeyValues
            }
          }
      }
    }
  }

    "expand Segment" when {
      "it contains removable key-values" in {
        runThis(10.times, log = true) {
          TestCaseSweeper {
            implicit sweeper =>

              implicit val pathsDistributor = createPathDistributor

              val segments = ListBuffer.range(0, 5).map(_ => TestSegment(keyValues = Slice(Memory.remove(1), Memory.remove(2), Memory.update(3))))
              segments.foreach(_.hasUpdateOrRange shouldBe true)

              implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
              implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

              val resultFragments =
                DefragGap.run[S](
                  gap = segments,
                  fragments = ListBuffer.empty,
                  removeDeletes = true,
                  createdInLevel = 1,
                  hasNext = false
                )

              resultFragments shouldBe empty
          }
        }
      }
    }
}
