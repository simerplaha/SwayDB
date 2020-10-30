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

package swaydb.core.segment

import java.nio.file.NoSuchFileException

import org.scalamock.scalatest.MockFactory
import swaydb.core.CommonAssertions._
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core.data.{Memory, Persistent}
import swaydb.core.io.file.Effect
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestTimer}
import swaydb.data.RunThis._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.{Error, IO}

class SegmentRefSpec extends TestBase with MockFactory {

  implicit val keyOrder = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))
  implicit val persistentKeyOrder: KeyOrder[Persistent] = KeyOrder(Ordering.by[Persistent, Slice[Byte]](_.key)(keyOrder))
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def segmentIO = SegmentIO.random
  implicit val ec = TestExecutionContext.executionContext

  "fastPutMany" should {
    "succeed" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val segmentKeyValues: Slice[Memory] = randomKeyValues(100)

            val segment: Segment = TestSegment(segmentKeyValues)

            val maxKey = segment.maxKey.maxKey.readInt[Byte]()
            val otherKeyValues: Slice[Memory] = randomKeyValues(startId = Some(maxKey), count = 1000)

            val assignables: Iterable[Iterable[Assignable]] = Seq(Seq(segment), otherKeyValues)

            val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
            val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
            val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
            val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
            val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
            val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random(mmap = mmapSegments)
            val pathDistributor = createPathDistributor

            val putResult =
              SegmentRef.fastPutMany(
                assignables = assignables,
                segmentParallelism = randomParallelMerge().ofSegment,
                removeDeletes = false,
                createdInLevel = 0,
                valuesConfig = valuesConfig,
                sortedIndexConfig = sortedIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                hashIndexConfig = hashIndexConfig,
                bloomFilterConfig = bloomFilterConfig,
                segmentConfig = segmentConfig,
                pathsDistributor = pathDistributor
              ).get

            val allKeyValues = segmentKeyValues ++ otherKeyValues

            //since there is one segment in the assignable that segment is always copied so
            val totalSegments = putResult.foldLeft(0)(_ + _.size)
            totalSegments should be >= 2
            putResult.head should have size 1
            putResult.head.head.iterator().toList shouldBe segmentKeyValues

            putResult.foreach(_.foreach(_.sweep()))
            putResult.flatMap(_.flatMap(_.iterator())) shouldBe allKeyValues
        }
      }
    }

    "fail" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val segmentKeyValues1: Slice[Memory] = randomKeyValues(100)
            val segment1: Segment = TestSegment(segmentKeyValues1)
            val maxKey1 = segment1.maxKey.maxKey.readInt[Byte]()

            val segmentKeyValues2: Slice[Memory] = randomKeyValues(100)
            val segment2: Segment = TestSegment(segmentKeyValues2)
            segment2.delete

            val otherKeyValues: Slice[Memory] = randomKeyValues(startId = Some(maxKey1), count = 1000)

            val assignables: Iterable[Iterable[Assignable]] =
              eitherOne(
                Seq(Seq(segment1, segment2), otherKeyValues),
                Seq(Seq(segment1), Seq(segment2), otherKeyValues),
                Seq(Seq(segment1), otherKeyValues ++ Slice(segment2))
              )

            val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
            val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
            val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
            val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
            val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
            val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random(mmap = mmapSegments)
            val pathDistributor = createPathDistributor.sweep(_.headPath.sweep())

            val putResult: IO[Error.Segment, Iterable[Slice[PersistentSegment]]] =
              SegmentRef.fastPutMany(
                assignables = assignables,
                segmentParallelism = randomParallelMerge().ofSegment,
                removeDeletes = false,
                createdInLevel = 0,
                valuesConfig = valuesConfig,
                sortedIndexConfig = sortedIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                hashIndexConfig = hashIndexConfig,
                bloomFilterConfig = bloomFilterConfig,
                segmentConfig = segmentConfig,
                pathsDistributor = pathDistributor
              )

            putResult.left.get.exception shouldBe a[NoSuchFileException]

            Effect.isEmptyOrNotExists(pathDistributor.headPath).get shouldBe true
        }
      }
    }
  }
}
