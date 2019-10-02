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
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
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

  val keyValueCount = 100

  import keyOrder._

  "transfer - tested via close for coverage" should {
    "transfer the last segment's KeyValues to previous segment, if the last segment's segmentSize is < minSegmentSize for persistent key-values" in {
      def doTest(inMemory: Boolean) = {
        implicit val testTimer: TestTimer = TestTimer.Empty

        val segment1 = SegmentBuffer()
        segment1 add Transient.put(key = 1, value = Some(1), previous = segment1.lastOption)
        segment1 add Transient.put(key = 2, value = Some(2), previous = segment1.lastOption) //total segmentSize is 144.bytes

        val smallerLastSegment = SegmentBuffer()
        smallerLastSegment add Transient.put(key = 3, value = Some(3), previous = None) //total segmentSize is 105.bytes

        val segments = ListBuffer[SegmentBuffer](segment1, smallerLastSegment)

        //minSegmentSize is 144.bytes but lastSegment size is 105.bytes. Expected result should move lastSegment's KeyValues to previous segment
        val newSegments =
          SegmentMerger.close(
            buffers = segments,
            minSegmentSize = if (inMemory) smallerLastSegment.last.stats.memorySegmentSize + 1 else smallerLastSegment.last.stats.segmentSize + 1,
            forMemory = inMemory,
            valuesConfig = ValuesBlock.Config.random,
            sortedIndexConfig = SortedIndexBlock.Config.random,
            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            hashIndexConfig = HashIndexBlock.Config.random,
            bloomFilterConfig = BloomFilterBlock.Config.random,
            createdInLevel = randomIntMax()
          )

        newSegments.size shouldBe 1

        val newSegmentsUnzipped = newSegments.head.toList
        newSegmentsUnzipped(0).key equiv segment1.head.key
        newSegmentsUnzipped(1).key equiv segment1.last.key
        newSegmentsUnzipped(2).key equiv smallerLastSegment.head.key
      }

      runThis(50.times) {
        doTest(inMemory = false)
        doTest(inMemory = true)
      }
    }

    "make no change if there is only one segment" in {
      runThisParallel(100.times) {
        val buffer = SegmentBuffer()
        (1 to 100) foreach {
          i =>
            buffer add randomFixedTransientKeyValue(i)
        }

        SegmentMerger.close(
          buffers = ListBuffer(buffer),
          minSegmentSize = buffer.last.stats.memorySegmentSize * 2,
          forMemory = true,
          valuesConfig = ValuesBlock.Config.random,
          sortedIndexConfig = SortedIndexBlock.Config.random,
          binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
          hashIndexConfig = HashIndexBlock.Config.random,
          bloomFilterConfig = BloomFilterBlock.Config.random,
          createdInLevel = randomIntMax()
        ).size shouldBe 1

        SegmentMerger.close(
          buffers = ListBuffer(buffer),
          minSegmentSize = buffer.last.stats.segmentSize * 2,
          forMemory = false,
          valuesConfig = ValuesBlock.Config.random,
          sortedIndexConfig = SortedIndexBlock.Config.random,
          binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
          hashIndexConfig = HashIndexBlock.Config.random,
          bloomFilterConfig = BloomFilterBlock.Config.random,
          createdInLevel = randomIntMax()
        ).size shouldBe 1
      }
    }
  }

  "close" should {
    "split KeyValues into equal chunks" in {

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
          createdInLevel = randomIntMax()
        ).toArray
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
          createdInLevel = randomIntMax()
        ).toArray
      )
    }
  }

  "split" should {
    "split key-values" in {

      runThis(100.times) {
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
            createdInLevel = randomIntMax()
          )

        splits should have size 5
        splits.map(_.toMemory) should contain only
          (Slice(Memory.put(1, 1)),
            Slice(Memory.remove(2)),
            Slice(Memory.put(3, 3)),
            Slice(Memory.put(4, 4)), //51.byte Segment size
            Slice(Memory.Range(5, 10, Some(Value.remove(None)), Value.update(5))) //56.bytes (segment size)
          )

        val transientKeyValues =
          keyValues.toTransient

        val persistentSplit =
          SegmentMerger.split(
            keyValues = keyValues,
            //* 5 because the size of hashIndex and bloomFilter are not calculated.
            minSegmentSize = transientKeyValues.last.stats.segmentSize * 5,
            isLastLevel = false,
            forInMemory = false,
            valuesConfig = transientKeyValues.last.valuesConfig,
            sortedIndexConfig = transientKeyValues.last.sortedIndexConfig,
            binarySearchIndexConfig = transientKeyValues.last.binarySearchIndexConfig,
            hashIndexConfig = transientKeyValues.last.hashIndexConfig,
            bloomFilterConfig = transientKeyValues.last.bloomFilterConfig,
            createdInLevel = randomIntMax()
          )

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
            createdInLevel = randomIntMax()
          )

        memorySplit should have size 1

        memorySplit.flatten shouldBe expected
      }
    }
  }

}
