/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.core.segment.merge

import swaydb.core.CommonAssertions._
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.segment.format.a.block._
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable.ListBuffer

class SegmentMergeSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit def groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomGroupingStrategyOption(10)

  val keyValueCount = 100

  import keyOrder._

  "completeMerge" should {

    "transfer the last segment's KeyValues to previous segment, if the last segment's segmentSize is < minSegmentSize for persistent key-values" in {
      runThisParallel(10.times) {
        implicit val testTimer: TestTimer = TestTimer.Empty

        val segment1 = ListBuffer.empty[Transient]
        segment1.+=(Transient.put(key = 1, value = Some(1), previous = segment1.lastOption))
        segment1.+=(Transient.put(key = 2, value = Some(2), previous = segment1.lastOption)) //total segmentSize is 144.bytes

        val smallerLastSegment = ListBuffer.empty[Transient]
        smallerLastSegment.+=(Transient.put(key = 1, value = Some(1), previous = None)) //total segmentSize is 105.bytes

        val segments = ListBuffer[ListBuffer[Transient]](segment1, smallerLastSegment)

        //minSegmentSize is 144.bytes but lastSegment size is 105.bytes. Expected result should move lastSegment's KeyValues to previous segment
        val newSegments =
          SegmentMerger.completeMerge(
            segments = segments,
            minSegmentSize = 144.bytes,
            forMemory = false,
            valuesConfig = ValuesBlock.Config.random,
            sortedIndexConfig = SortedIndexBlock.Config.random,
            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            hashIndexConfig = HashIndexBlock.Config.random,
            bloomFilterConfig = BloomFilterBlock.Config.random,
            groupLastSegment = true,
            createdInLevel = randomIntMax()
          ).runRandomIO.value

        newSegments.size shouldBe 1

        val newSegmentsUnzipped = unzipGroups(newSegments.head)
        newSegmentsUnzipped(0).key equiv segment1.head.key
        newSegmentsUnzipped(1).key equiv segment1.last.key
        newSegmentsUnzipped(2).key equiv smallerLastSegment.head.key
      }
    }

    "transfer the last segment's KeyValues to previous segment, if the last segment's segmentSize is < minSegmentSize for memory key-values" in {
      runThisParallel(10.times) {
        val segment1 = ListBuffer.empty[Transient]
        segment1.+=(Transient.put(key = 1, value = Some(1), previous = segment1.lastOption))
        segment1.+=(Transient.put(key = 2, value = Some(2), previous = segment1.lastOption)) //total segmentSize is 21.bytes

        val smallerLastSegment = ListBuffer.empty[Transient]
        smallerLastSegment.+=(Transient.put(key = 1, value = Some(1), previous = None)) //total segmentSize is 12.bytes

        val segments = ListBuffer[ListBuffer[Transient]](segment1, smallerLastSegment)

        //minSegmentSize is 21.bytes but lastSegment size is 12.bytes. Expected result should move lastSegment's KeyValues to previous segment
        val newSegments =
          SegmentMerger.completeMerge(
            segments = segments,
            minSegmentSize = 21.bytes,
            forMemory = true,
            valuesConfig = ValuesBlock.Config.random,
            sortedIndexConfig = SortedIndexBlock.Config.random,
            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            hashIndexConfig = HashIndexBlock.Config.random,
            bloomFilterConfig = BloomFilterBlock.Config.random,
            groupLastSegment = true,
            createdInLevel = randomIntMax()
          ).runRandomIO.value

        newSegments.size shouldBe 1

        val newSegmentsUnzipped = unzipGroups(newSegments.head)
        newSegmentsUnzipped(0).key equiv segment1.head.key
        newSegmentsUnzipped(1).key equiv segment1.last.key
        newSegmentsUnzipped(2).key equiv smallerLastSegment.head.key
      }
    }

    "make no change if there is only one segment" in {
      runThisParallel(100.times) {
        val segment: ListBuffer[Transient] = ListBuffer(randomizedKeyValues(randomIntMax(5) max 1, addPendingApply = true, addGroups = false).toList: _*)

        SegmentMerger.completeMerge(
          segments = ListBuffer(segment),
          minSegmentSize = randomIntMax(segment.last.stats.segmentSize),
          forMemory = false,
          valuesConfig = ValuesBlock.Config.random,
          sortedIndexConfig = SortedIndexBlock.Config.random,
          binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
          hashIndexConfig = HashIndexBlock.Config.random,
          bloomFilterConfig = BloomFilterBlock.Config.random,
          groupLastSegment = true,
          createdInLevel = randomIntMax()
        ).runRandomIO.value.size shouldBe 1

        SegmentMerger.completeMerge(
          segments = ListBuffer(segment),
          minSegmentSize = randomIntMax(segment.last.stats.memorySegmentSize),
          forMemory = true,
          valuesConfig = ValuesBlock.Config.random,
          sortedIndexConfig = SortedIndexBlock.Config.random,
          binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
          hashIndexConfig = HashIndexBlock.Config.random,
          bloomFilterConfig = BloomFilterBlock.Config.random,
          groupLastSegment = true,
          createdInLevel = randomIntMax()
        ).runRandomIO.value.size shouldBe 1
      }
    }

    "split KeyValues into equal chunks" in {

      implicit val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = None

      val oldKeyValues: Slice[Memory] = Slice(Memory.put(1, 1), Memory.put(2, 2), Memory.put(3, 3), Memory.put(4, 4))
      val newKeyValues: Slice[Memory] = Slice(Memory.put(1, 22), Memory.put(2, 22), Memory.put(3, 22), Memory.put(4, 22))

      def assert(segments: Array[Iterable[Transient]]) = {
        segments.length shouldBe 4

        segments(0).size shouldBe 1
        segments(0).head shouldBe newKeyValues(0)

        segments(1).size shouldBe 1
        segments(1).head.key equiv newKeyValues(1).key

        segments(2).size shouldBe 1
        segments(2).head.key equiv newKeyValues(2).key

        segments(3).size shouldBe 1
        segments(3).head.key equiv newKeyValues(3).key
      }

      assert(
        SegmentMerger.merge(
          newKeyValues = newKeyValues,
          oldKeyValues = oldKeyValues,
          minSegmentSize = 1.byte,
          isLastLevel = false,
          forInMemory = false,
          valuesConfig = ValuesBlock.Config.random,
          sortedIndexConfig = SortedIndexBlock.Config.random,
          binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
          hashIndexConfig = HashIndexBlock.Config.random,
          bloomFilterConfig = BloomFilterBlock.Config.random,
          segmentIO = SegmentIO.random,
          createdInLevel = randomIntMax()
        ).runRandomIO.value.toArray
      )

      assert(
        SegmentMerger.merge(
          newKeyValues = newKeyValues,
          oldKeyValues = oldKeyValues,
          minSegmentSize = 1.byte,
          isLastLevel = false,
          forInMemory = true,
          valuesConfig = ValuesBlock.Config.random,
          sortedIndexConfig = SortedIndexBlock.Config.random,
          binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
          hashIndexConfig = HashIndexBlock.Config.random,
          bloomFilterConfig = BloomFilterBlock.Config.random,
          segmentIO = SegmentIO.random,
          createdInLevel = randomIntMax()
        ).runRandomIO.value.toArray
      )
    }
  }

  "split" should {
    "split key-values" in {

      implicit val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = None

      val keyValues: Slice[Memory] = Slice(Memory.put(1, 1), Memory.remove(2), Memory.put(3, 3), Memory.put(4, 4), Memory.Range(5, 10, Some(Value.remove(None)), Value.update(5)))

      val splits =
        SegmentMerger.split(
          keyValues = keyValues,
          minSegmentSize = 1.byte,
          isLastLevel = false,
          forInMemory = false,
          valuesConfig = ValuesBlock.Config.random,
          sortedIndexConfig = SortedIndexBlock.Config.random,
          binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
          hashIndexConfig = HashIndexBlock.Config.random,
          bloomFilterConfig = BloomFilterBlock.Config.random,
          segmentIO = SegmentIO.random,
          createdInLevel = randomIntMax()
        ).runRandomIO.value

      splits should have size 5
      splits.map(_.toMemory) should contain only
        (Slice(Memory.put(1, 1)),
          Slice(Memory.remove(2)),
          Slice(Memory.put(3, 3)),
          Slice(Memory.put(4, 4)), //51.byte Segment size
          Slice(Memory.Range(5, 10, Some(Value.remove(None)), Value.update(5))) //56.bytes (segment size)
        )

      val persistentSplit =
        SegmentMerger.split(
          keyValues = keyValues,
          minSegmentSize = keyValues.toTransient.last.stats.segmentSize,
          isLastLevel = false,
          forInMemory = false,
          valuesConfig = ValuesBlock.Config.random,
          sortedIndexConfig = SortedIndexBlock.Config.random,
          binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
          hashIndexConfig = HashIndexBlock.Config.random,
          bloomFilterConfig = BloomFilterBlock.Config.random,
          segmentIO = SegmentIO.random,
          createdInLevel = randomIntMax()
        ).runRandomIO.value

      persistentSplit should have size 1

      val expected =
        Seq(
          Transient.put(1, 1),
          Transient.remove(2),
          Transient.put(3, 3),
          Transient.put(4, 4), //51.byte Segment size
          Transient.Range.create[FromValue, RangeValue](5, 10, Some(Value.remove(None)), Value.update(5)) //56.bytes (segment size)
        ).updateStats

      persistentSplit.flatten shouldBe expected

      val memorySplit =
        SegmentMerger.split(
          keyValues = keyValues,
          minSegmentSize = keyValues.toTransient.last.stats.memorySegmentSize,
          isLastLevel = false,
          forInMemory = true,
          valuesConfig = ValuesBlock.Config.random,
          sortedIndexConfig = SortedIndexBlock.Config.random,
          binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
          hashIndexConfig = HashIndexBlock.Config.random,
          bloomFilterConfig = BloomFilterBlock.Config.random,
          segmentIO = SegmentIO.random,
          createdInLevel = randomIntMax()
        ).runRandomIO.value

      memorySplit should have size 1

      memorySplit.flatten shouldBe expected
    }
  }

  "Merging fixed into Group" should {
    "return the same result as merging a list of Fixed key-values into Fixed" in {
      runThisParallel(10.times) {
        val fixedKeyValues = randomKeyValues(count = keyValueCount, addRemoves = true, startId = Some(1))
        val oldKeyValues = randomKeyValues(count = keyValueCount, startId = Some(fixedKeyValues.head.key.readInt()), addRemoves = true, addRanges = true)

        val mergeResultWithoutGroup =
          SegmentMerger.merge(
            newKeyValues = fixedKeyValues,
            oldKeyValues = oldKeyValues,
            minSegmentSize = 100.mb,
            isLastLevel = false,
            forInMemory = false,
            valuesConfig = ValuesBlock.Config.random,
            sortedIndexConfig = SortedIndexBlock.Config.random,
            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            hashIndexConfig = HashIndexBlock.Config.random,
            bloomFilterConfig = BloomFilterBlock.Config.random,
            segmentIO = SegmentIO.random,
            createdInLevel = randomIntMax()
          ).runRandomIO.value

        mergeResultWithoutGroup should have size 1

        val group =
          Transient.Group(
            keyValues = oldKeyValues,
            previous = None,
            valuesConfig = ValuesBlock.Config.random,
            sortedIndexConfig = SortedIndexBlock.Config.random,
            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            hashIndexConfig = HashIndexBlock.Config.random,
            bloomFilterConfig = BloomFilterBlock.Config.random,
            groupConfig = SegmentBlock.Config.random,
            createdInLevel = randomIntMax()
          ).runRandomIO.value.toMemory

        val mergeResultWithGroup =
          SegmentMerger.merge(
            newKeyValues = fixedKeyValues,
            oldKeyValues = Slice(group),
            minSegmentSize = 100.mb,
            isLastLevel = false,
            forInMemory = false,
            valuesConfig = ValuesBlock.Config.random,
            sortedIndexConfig = SortedIndexBlock.Config.random,
            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            hashIndexConfig = HashIndexBlock.Config.random,
            bloomFilterConfig = BloomFilterBlock.Config.random,
            segmentIO = SegmentIO.random,
            createdInLevel = randomIntMax()
          ).runRandomIO.value

        mergeResultWithGroup should have size 1

        mergeResultWithoutGroup.head shouldBe mergeResultWithGroup.head
      }
    }
  }
}
