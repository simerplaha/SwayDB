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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.ref

import org.scalamock.scalatest.MockFactory
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.{Memory, Persistent}
import swaydb.core.merge.MergeStats
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestTimer}
import swaydb.data.RunThis._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import org.scalatest.EitherValues._

import scala.collection.mutable.ListBuffer

class SegmentRefWriterSpec extends TestBase with MockFactory {

  implicit val keyOrder = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))
  implicit val persistentKeyOrder: KeyOrder[Persistent] = KeyOrder(Ordering.by[Persistent, Slice[Byte]](_.key)(keyOrder))
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def segmentIO = SegmentReadIO.random
  implicit val ec = TestExecutionContext.executionContext

  "fastPutMany" should {
    "succeed" in {
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
      ???
    }
  }


  "private functions" when {
    "buildGapStats" should {
      "return empty" when {
        "no gaps" in {
          val buffer = ListBuffer.empty[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], Segment]]

          //          SegmentRefWriter.defragGaps(
          //            gap = Iterable.empty,
          //            segments = buffer,
          //
          //          ).await

          buffer shouldBe empty
        }
      }

      //      "add Segment" in {
      //        runThis(10.times, log = true) {
      //          TestCaseSweeper {
      //            implicit sweeper =>
      //              //Segment  = [A - N]
      //              //Existing = EMPTY
      //
      //              val segment = TestSegment()
      //              segment.close
      //              segment.isOpen shouldBe false
      //
      //              val buffer = ListBuffer.empty[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], Segment]]
      //
      //              SegmentRefWriter.buildGapStats(
      //                gap = Seq(segment),
      //                sortedIndexConfig = SortedIndexBlock.Config.random,
      //                segmentConfig = SegmentBlock.Config.random,
      //                segments = buffer
      //              ).await
      //
      //              buffer should have size 1
      //              val headSegment = buffer.head.getOrElse(Segment.Null)
      //              headSegment shouldBe a[Segment]
      //              headSegment.hashCode() shouldBe segment.hashCode()
      //              headSegment.getS.path shouldBe segment.path
      //
      //              //does not open
      //              segment.isOpen shouldBe false
      //          }
      //        }
      //      }
    }
  }

}
