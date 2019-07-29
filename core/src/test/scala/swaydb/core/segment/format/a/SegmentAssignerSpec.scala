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

package swaydb.core.segment.format.a

import org.scalatest.OptionValues._
import swaydb.core.CommonAssertions._
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data.{KeyValue, Memory, Transient, Value}
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.IOEffect._
import swaydb.core.segment.format.a.block.SegmentIO
import swaydb.core.segment.{Segment, SegmentAssigner}
import swaydb.core.util.PipeOps._
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable
import scala.concurrent.duration._

class SegmentAssignerSpec0 extends SegmentAssignerSpec {
  val keyValueCount = 100
}

class SegmentAssignerSpec1 extends SegmentAssignerSpec {
  val keyValueCount = 100

  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentAssignerSpec2 extends SegmentAssignerSpec {
  val keyValueCount = 100

  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentAssignerSpec3 extends SegmentAssignerSpec {
  val keyValueCount = 1000
  override def inMemoryStorage = true
}

sealed trait SegmentAssignerSpec extends TestBase {
  implicit val keyOrder = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit def segmentIO: SegmentIO = SegmentIO.random

  def keyValueCount: Int

  val groupingStrategy: Option[KeyValueGroupingStrategyInternal] =
    randomGroupingStrategyOption(keyValueCount)

  "SegmentAssign.assign" should {

    "assign KeyValues to the first Segment if there is only one Segment" in {
      val keyValues = randomizedKeyValues(keyValueCount).toMemory

      val segment = TestSegment().runRandomIO.value

      val result = SegmentAssigner.assign(keyValues, List(segment)).runRandomIO.value
      result.size shouldBe 1
      result.keys.head.path shouldBe segment.path
      result.values.head shouldBe keyValues
    }

    "assign a KeyValue and then a Group that spread over multiple Segments" in {
      //this test asserts for when Group's expansion results in ArrayIndexOutOfBoundsException
      //when inserting assigned key-values to a Segment in SegmentAssigner.
      val fixed = randomFixedKeyValue(1)
      val group = randomGroup(randomKeyValues(count = 1000, startId = Some(2))).toMemory
      val newKeyValues = Seq(fixed, group).toTransient

      val segment1 = TestSegment(randomKeyValues(startId = Some(1))).runRandomIO.value
      val segment2 = TestSegment(randomKeyValues(startId = Some(10))).runRandomIO.value

      val result = SegmentAssigner.assign(newKeyValues, List(segment1, segment2)).runRandomIO.value
      result.size shouldBe 2
    }

    "assign KeyValues to second Segment when none of the keys belong to the first Segment" in {
      val segment1 = TestSegment(Slice(Transient.put(1), Transient.Range.create[FromValue, RangeValue](2, 10, None, Value.remove(10.seconds.fromNow))).updateStats).runRandomIO.value
      val segment2 = TestSegment(Slice(Transient.put(10)).updateStats).runRandomIO.value
      val segments = Seq(segment1, segment2)

      val result =
        SegmentAssigner.assign(
          keyValues =
            Slice(
              randomFixedKeyValue(10),
              randomRangeKeyValue(11, 20),
              randomFixedKeyValue(20),
              randomGroup(Slice(randomFixedKeyValue(30), randomRangeKeyValue(40, 50)).toTransient).toMemory
            ),
          segments = segments
        ).runRandomIO.value

      result.size shouldBe 1
      result.keys.head.path shouldBe segment2.path
    }

    "assign gap KeyValue to the first Segment if the first Segment already has a key-value assigned to it" in {
      val segment1 = TestSegment(Slice(randomFixedKeyValue(1), randomRangeKeyValue(2, 10)).toTransient).runRandomIO.value
      val segment2 = TestSegment(Slice(randomFixedKeyValue(20)).toTransient).runRandomIO.value
      val segments = Seq(segment1, segment2)

      //1 belongs to first Segment, 15 is a gap key and since first segment is not empty, it will value assigned 15.
      val keyValues =
        Slice(
          Memory.put(1, 1),
          Memory.put(15),
          randomGroup(Slice(Memory.remove(16), Memory.Range(17, 18, None, Value.update("update"))).toTransient).toMemory,
          Memory.Range(16, 20, None, Value.update(16))
        )

      val result = SegmentAssigner.assign(keyValues, segments).runRandomIO.value
      result.size shouldBe 1
      result.keys.head.path shouldBe segment1.path
      result.values.head.toMemory shouldBe keyValues
    }

    "assign gap KeyValue to the second Segment if the first Segment has no key-value assigned to it" in {
      runThis(10.times) {
        val segment1KeyValues = Slice(randomFixedKeyValue(1), randomRangeKeyValue(2, 10))
        val segment2KeyValues = Slice(randomFixedKeyValue(20))

        val segment1 = TestSegment(segment1KeyValues.toTransient).runRandomIO.value
        val segment2 = TestSegment(segment2KeyValues.toTransient).runRandomIO.value
        val segments = Seq(segment1, segment2)

        //15 is a gap key but no key-values are assigned to segment1 so segment2 will value this key-value.
        val keyValues =
          Slice(
            randomFixedKeyValue(15),
            randomGroup(Slice(randomFixedKeyValue(16), randomRangeKeyValue(17, 18)).toTransient).toMemory,
            randomRangeKeyValue(20, 100)
          )

        val result = SegmentAssigner.assign(keyValues, segments).runRandomIO.value
        result.size shouldBe 1
        result.keys.head.path shouldBe segment2.path
        result.values.head.toMemory shouldBe keyValues
      }
    }

    "assign gap Range KeyValue to all Segments that fall within the Range's toKey" in {
      // 1 - 10(exclusive)
      val segment1 = TestSegment(Slice(Transient.put(1), Transient.Range.create[FromValue, RangeValue](2, 10, None, Value.remove(None))).updateStats).runRandomIO.value
      // 20 - 20
      val segment2 = TestSegment(Slice(Transient.remove(20)).updateStats).runRandomIO.value
      //21 - 30
      val segment3 = TestSegment(Slice(Transient.Range.create[FromValue, RangeValue](21, 30, None, Value.remove(None)), Transient.put(30)).updateStats).runRandomIO.value
      //40 - 60
      val segment4 = TestSegment(Slice(Transient.remove(40), Transient.Range.create[FromValue, RangeValue](41, 50, None, Value.remove(None)), Transient.put(60)).updateStats).runRandomIO.value
      //70 - 80
      val segment5 = TestSegment(Slice(Transient.put(70), Transient.remove(80)).updateStats).runRandomIO.value
      val segments = Seq(segment1, segment2, segment3, segment4, segment5)

      //15 is a gap key but no key-values are assigned to segment1 so segment2 will value this key-value an it will be split across.
      //all next overlapping Segments.
      val keyValues =
      Slice(
        Memory.Range(15, 50, Some(Value.remove(None)), Value.update(10))
      )

      def assertResult(assignments: mutable.Map[Segment, Slice[KeyValue.ReadOnly]]) = {
        assignments.size shouldBe 3
        assignments.find(_._1 == segment2).value._2 should contain only Memory.Range(15, 21, Some(Value.remove(None)), Value.update(10))
        assignments.find(_._1 == segment3).value._2 should contain only Memory.Range(21, 40, None, Value.update(10))
        assignments.find(_._1 == segment4).value._2 should contain only Memory.Range(40, 50, None, Value.update(10))
      }

      assertResult(SegmentAssigner.assign(keyValues, segments).runRandomIO.value)

      //group should also result in same.
      val grouped = randomGroup(keyValues.toTransient).toMemory
      assertResult(SegmentAssigner.assign(Slice(grouped), segments).runRandomIO.value)
    }

    "assign key value to the first segment when the key is the new smallest" in {
      val segment1 = TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)).toTransient).runRandomIO.value
      val segment2 = TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).runRandomIO.value

      //segment1 - 1 - 2
      //segment2 - 4 - 5
      val segments = Seq(segment1, segment2)

      SegmentAssigner.assign(Slice(Memory.put(0)), segments).runRandomIO.value ==> {
        result =>
          result.size shouldBe 1
          result.keys.head.path shouldBe segment1.path
      }
    }

    "assign key value to the first segment and split out to other Segment when the key is the new smallest and the range spreads onto other Segments" in {
      val segment1 = TestSegment(Slice(Transient.put(1), Transient.put(2)).updateStats).runRandomIO.value
      val segment2 = TestSegment(Slice(Transient.put(4), Transient.put(5)).updateStats).runRandomIO.value
      val segment3 = TestSegment(Slice(Transient.Range.create[FromValue, RangeValue](6, 10, Some(Value.remove(None)), Value.update(10)), Transient.remove(10)).updateStats).runRandomIO.value

      //segment1 - 1 - 2
      //segment2 - 4 - 5
      //segment3 - 6 - 10
      val segments = Seq(segment1, segment2, segment3)

      //insert range 0 - 20. This overlaps all 3 Segment and key-values will value sliced and distributed to all Segments.
      SegmentAssigner.assign(Slice(Memory.Range(0, 20, Some(Value.put(0)), Value.remove(None))), segments).runRandomIO.value ==> {
        assignments =>
          assignments.size shouldBe 3
          assignments.find(_._1 == segment1).value._2 should contain only Memory.Range(0, 4, Some(Value.put(0)), Value.remove(None))
          assignments.find(_._1 == segment2).value._2 should contain only Memory.Range(4, 6, None, Value.remove(None))
          assignments.find(_._1 == segment3).value._2 should contain only Memory.Range(6, 20, None, Value.remove(None))
      }
    }

    "debugger" in {
      val segment1 = TestSegment(Slice(Memory.put(1), Memory.Range(26074, 26075, None, Value.update(None, None))).toTransient).runRandomIO.value
      val segment2 = TestSegment(Slice(Memory.put(26075), Memory.Range(28122, 28123, None, Value.update(None, None))).toTransient).runRandomIO.value
      val segment3 = TestSegment(Slice(Memory.put(28123), Memory.Range(32218, 32219, None, Value.update(None, None))).toTransient).runRandomIO.value
      val segment4 = TestSegment(Slice(Memory.put(32219), Memory.Range(40410, 40411, None, Value.update(None, None))).toTransient).runRandomIO.value
      val segment5 = TestSegment(Slice(Memory.put(74605), Memory.put(100000)).toTransient).runRandomIO.value

      val segments = Seq(segment1, segment2, segment3, segment4, segment5)

      SegmentAssigner.assign(Slice(Memory.put(1), Memory.put(100000)), segments).runRandomIO.value ==> {
        assignments =>
          assignments.size shouldBe 2
          assignments.find(_._1 == segment1).value._2 should contain only Memory.put(1)
          assignments.find(_._1 == segment5).value._2 should contain only Memory.put(100000)
      }
    }

    "assign key value to the last segment when the key is the new largest" in {
      val segment1 = TestSegment(Slice(Transient.put(1), Transient.put(2)).updateStats).runRandomIO.value
      val segment2 = TestSegment(Slice(Transient.put(4), Transient.put(5)).updateStats).runRandomIO.value
      val segment3 = TestSegment(Slice(Transient.put(6), Transient.put(7)).updateStats).runRandomIO.value
      val segment4 = TestSegment(Slice(Transient.put(8), Transient.put(9)).updateStats).runRandomIO.value
      val segments = Seq(segment1, segment2, segment3, segment4)

      SegmentAssigner.assign(Slice(Memory.put(10, "ten")), segments).runRandomIO.value ==> {
        result =>
          result.size shouldBe 1
          result.keys.head.path shouldBe segment4.path
          result.values.head should contain only Memory.put(10, "ten")
      }

      SegmentAssigner.assign(Slice(Memory.remove(10)), segments).runRandomIO.value ==> {
        result =>
          result.size shouldBe 1
          result.keys.head.path shouldBe segment4.path
          result.values.head should contain only Memory.remove(10)
      }

      SegmentAssigner.assign(Slice(Memory.Range(10, 20, Some(Value.put(10)), Value.remove(None))), segments).runRandomIO.value ==> {
        result =>
          result.size shouldBe 1
          result.keys.head.path shouldBe segment4.path
          result.values.head should contain only Memory.Range(10, 20, Some(Value.put(10)), Value.remove(None))
      }

      SegmentAssigner.assign(Slice(randomGroup(Slice(Memory.Range(10, 20, Some(Value.put(10)), Value.remove(None))).toTransient).toMemory), segments).runRandomIO.value ==> {
        result =>
          result.size shouldBe 1
          result.keys.head.path shouldBe segment4.path
          unzipGroups(result.values.head).head shouldBe Memory.Range(10, 20, Some(Value.put(10)), Value.remove(None))
      }
    }

    "assign all KeyValues to their target Segments" in {
      val keyValues = Slice(randomFixedKeyValue(1), randomFixedKeyValue(2), randomFixedKeyValue(3), randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient
      val segment1 = TestSegment(Slice(randomFixedKeyValue(key = 1)).toTransient).runRandomIO.value
      val segment2 = TestSegment(Slice(randomFixedKeyValue(key = 2)).toTransient).runRandomIO.value
      val segment3 = TestSegment(Slice(randomFixedKeyValue(key = 3)).toTransient).runRandomIO.value
      val segment4 = TestSegment(Slice(randomFixedKeyValue(key = 4)).toTransient).runRandomIO.value
      val segment5 = TestSegment(Slice(randomFixedKeyValue(key = 5)).toTransient).runRandomIO.value

      val segments = List(segment1, segment2, segment3, segment4, segment5)

      val result = SegmentAssigner.assign(keyValues.toMemory, segments).runRandomIO.value
      result.size shouldBe 5

      //sort them by the fileId, so it's easier to test
      val resultArray = result.toArray.sortBy(_._1.path.fileId.runRandomIO.value._1)

      resultArray(0)._1.path shouldBe segment1.path
      resultArray(0)._2 should have size 1
      resultArray(0)._2.head.key shouldBe (1: Slice[Byte])

      resultArray(1)._1.path shouldBe segment2.path
      resultArray(1)._2 should have size 1
      resultArray(1)._2.head.key shouldBe (2: Slice[Byte])

      resultArray(2)._1.path shouldBe segment3.path
      resultArray(2)._2 should have size 1
      resultArray(2)._2.head.key shouldBe (3: Slice[Byte])

      resultArray(3)._1.path shouldBe segment4.path
      resultArray(3)._2 should have size 1
      resultArray(3)._2.head.key shouldBe (4: Slice[Byte])

      resultArray(4)._1.path shouldBe segment5.path
      resultArray(4)._2 should have size 1
      resultArray(4)._2.head.key shouldBe (5: Slice[Byte])
    }
  }
}
