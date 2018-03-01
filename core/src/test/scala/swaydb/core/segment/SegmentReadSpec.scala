/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment

import java.nio.file.{Files, NoSuchFileException}

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.core.TestBase
import swaydb.core.data.{KeyValue, Transient}
import swaydb.core.data.Transient.Remove
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.one.SegmentWriter
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

//@formatter:off
class SegmentReadSpec1 extends SegmentReadSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentReadSpec2 extends SegmentReadSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentReadSpec3 extends SegmentReadSpec {
  override def inMemoryStorage = true
}
//@formatter:on

class SegmentReadSpec extends TestBase with ScalaFutures with PrivateMethodTester {

  implicit val ordering = KeyOrder.default
  val keyValuesCount = 10

  "Segment.belongsTo" should {
    "return true for overlapping KeyValues else false" in {
      val segment = TestSegment(Slice(Transient.Put(1), Remove(5)).updateStats).assertGet

      Segment.belongsTo(Transient.Put(0), segment) shouldBe false

      Segment.belongsTo(Transient.Put(1), segment) shouldBe true
      Segment.belongsTo(Transient.Put(2), segment) shouldBe true
      Segment.belongsTo(Remove(3), segment) shouldBe true
      Segment.belongsTo(Transient.Put(4), segment) shouldBe true
      Segment.belongsTo(Remove(5), segment) shouldBe true

      Segment.belongsTo(Remove(6), segment) shouldBe false

      segment.close.assertGet

    }
  }

  "Segment.rangeBelongsTo" should {
    "return true for overlapping KeyValues else false" in {
      val segment = TestSegment(Slice(Transient.Put(1), Remove(5)).updateStats).assertGet

      //      1 - 5
      //      1 - 5
      Segment.overlaps(1, 5, segment) shouldBe true
      //0 - 0
      //      1 - 5
      Segment.overlaps(0, 0, segment) shouldBe false
      //  0 - 1
      //      1 - 5
      Segment.overlaps(0, 1, segment) shouldBe true
      //    0 - 2
      //      1 - 5
      Segment.overlaps(0, 2, segment) shouldBe true
      //       2-4
      //      1 - 5
      Segment.overlaps(2, 4, segment) shouldBe true
      //        4 - 6
      //      1 - 5
      Segment.overlaps(4, 6, segment) shouldBe true
      //          5 - 6
      //      1 - 5
      Segment.overlaps(5, 6, segment) shouldBe true
      //            6 - 7
      //      1 - 5
      Segment.overlaps(6, 7, segment) shouldBe false

      //wide outer overlap
      //    0   -   6
      //      1 - 5
      Segment.overlaps(0, 6, segment) shouldBe true

      segment.close.assertGet
    }
  }

  "Segment.overlaps" should {
    "return true for overlapping Segments else false" in {
      //0 1
      //    2 3
      var segment1 = TestSegment(Slice(Transient.Put(0), Remove(1)).updateStats).assertGet
      var segment2 = TestSegment(Slice(Remove(2), Transient.Put(3)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //1 2
      //  2 3
      segment1 = TestSegment(Slice(Transient.Put(1), Remove(2)).updateStats).assertGet
      segment2 = TestSegment(Slice(Remove(2), Transient.Put(3)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //2 3
      //2 3
      segment1 = TestSegment(Slice(Remove(2), Transient.Put(3)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Put(2), Remove(3)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //  3 4
      //2 3
      segment1 = TestSegment(Slice(Remove(3), Transient.Put(4)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Put(2), Remove(3)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //    4 5
      //2 3
      segment1 = TestSegment(Slice(Transient.Put(4), Remove(5)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Put(2), Remove(3)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //0       10
      //   2 3
      segment1 = TestSegment(Slice(Remove(0), Transient.Put(10)).updateStats).assertGet
      segment2 = TestSegment(Slice(Remove(2), Transient.Put(3)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //   2 3
      //0       10
      segment1 = TestSegment(Slice(Transient.Put(2), Transient.Put(3)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Put(0), Transient.Put(10)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      segment1.close.assertGet
      segment2.close.assertGet
    }
  }

  "Segment.nonOverlapping and overlapping" should {
    "return non overlapping Segments" in {
      //0-1, 2-3
      //         4-5, 6-7
      var segments1 = List(TestSegment(Slice(Transient.Put(0), Transient.Put(1)).updateStats).get, TestSegment(Slice(Transient.Put(2), Transient.Put(3)).updateStats).get)
      var segments2 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) shouldBe segments1.map(_.path)
      Segment.nonOverlapping(segments2, segments1).map(_.path) shouldBe segments2.map(_.path)
      Segment.overlaps(segments1, segments2).map(_.path) shouldBe empty
      Segment.overlaps(segments2, segments1).map(_.path) shouldBe empty


      //2-3, 4-5
      //     4-5, 6-7
      segments1 = List(TestSegment(Slice(Transient.Put(2), Transient.Put(3)).updateStats).get, TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get)
      segments2 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) should contain only segments1.head.path
      Segment.nonOverlapping(segments2, segments1).map(_.path) should contain only segments2.last.path
      Segment.overlaps(segments1, segments2).map(_.path) should contain only segments1.last.path
      Segment.overlaps(segments2, segments1).map(_.path) should contain only segments2.head.path

      //4-5, 6-7
      //4-5, 6-7
      segments1 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      segments2 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) shouldBe empty
      Segment.nonOverlapping(segments2, segments1).map(_.path) shouldBe empty
      Segment.overlaps(segments1, segments2).map(_.path) shouldBe segments1.map(_.path)
      Segment.overlaps(segments2, segments1).map(_.path) shouldBe segments2.map(_.path)

      //     6-7, 8-9
      //4-5, 6-7
      segments1 = List(TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get, TestSegment(Slice(Transient.Put(8), Transient.Put(9)).updateStats).get)
      segments2 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) should contain only segments1.last.path
      Segment.nonOverlapping(segments2, segments1).map(_.path) should contain only segments2.head.path
      Segment.overlaps(segments1, segments2).map(_.path) should contain only segments1.head.path
      Segment.overlaps(segments2, segments1).map(_.path) should contain only segments2.last.path

      //         8-9, 10-11
      //4-5, 6-7
      segments1 = List(TestSegment(Slice(Transient.Put(8), Transient.Put(9)).updateStats).get, TestSegment(Slice(Transient.Put(10), Transient.Put(11)).updateStats).get)
      segments2 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) should contain allElementsOf segments1.map(_.path)
      Segment.nonOverlapping(segments2, segments1).map(_.path) should contain allElementsOf segments2.map(_.path)
      Segment.overlaps(segments1, segments2).map(_.path) shouldBe empty
      Segment.overlaps(segments2, segments1).map(_.path) shouldBe empty

      //1-2            10-11
      //     4-5, 6-7
      segments1 = List(TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats).get, TestSegment(Slice(Transient.Put(10), Transient.Put(11)).updateStats).get)
      segments2 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) should contain allElementsOf segments1.map(_.path)
      Segment.nonOverlapping(segments2, segments1).map(_.path) should contain allElementsOf segments2.map(_.path)
      Segment.overlaps(segments1, segments2).map(_.path) shouldBe empty
      Segment.overlaps(segments2, segments1).map(_.path) shouldBe empty
    }
  }

  "Segment.tempMinMaxKeyValues" should {
    "return key-values with Segments min and max keys only" in {
      val segments = Seq(TestSegment().assertGet, TestSegment().assertGet, TestSegment().assertGet, TestSegment().assertGet)

      Segment.tempMinMaxKeyValues(segments) shouldBe
        Slice(
          Transient.Put(segments(0).minKey), Transient.Put(segments(0).maxKey),
          Transient.Put(segments(1).minKey), Transient.Put(segments(1).maxKey),
          Transient.Put(segments(2).minKey), Transient.Put(segments(2).maxKey),
          Transient.Put(segments(3).minKey), Transient.Put(segments(3).maxKey)
        )
    }
  }
  //
  "Segment.overlapsWithBusySegments" should {
    "return true or false if input Segments overlap or do not overlap with busy Segments respectively" in {

      val targetSegments = {
        TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats) ::
          TestSegment(Slice(Transient.Put(3), Transient.Put(4)).updateStats) ::
          TestSegment(Slice(Transient.Put(7), Transient.Put(8)).updateStats) ::
          TestSegment(Slice(Transient.Put(9), Transient.Put(10)).updateStats) ::
          Nil
      }.map(_.assertGet)

      //0-1
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      var inputSegments = Seq(TestSegment(Slice(Transient.Put(0), Transient.Put(1)).updateStats)).map(_.assertGet)
      var busySegments = Seq(TestSegment(Slice(Transient.Put(3), Transient.Put(4)).updateStats), TestSegment(Slice(Transient.Put(7), Transient.Put(8)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe false

      //     1-2
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe false

      //          3-4
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(Transient.Put(3), Transient.Put(2)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe true

      //               5-6
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(Transient.Put(5), Transient.Put(6)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe true

      //                         9-10
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(Transient.Put(9), Transient.Put(10)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe false

      //               5-6
      //     1-2            7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(Transient.Put(5), Transient.Put(6)).updateStats)).map(_.assertGet)
      busySegments = {
        TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats) ::
          TestSegment(Slice(Transient.Put(7), Transient.Put(8)).updateStats) ::
          Nil
      }.map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe true

      //               5-6
      //     1-2                 9-10
      //     1-2, 3-4, ---, 7-8, 9-10
      busySegments = Seq(TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats), TestSegment(Slice(Transient.Put(9), Transient.Put(10)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe false
    }
  }

  "Segment.getAllKeyValues" should {
    "get KeyValues from multiple Segments" in {
      val keyValues1 = randomIntKeyValues(keyValuesCount, addRandomDeletes = true)
      val keyValues2 = randomIntKeyValues(keyValuesCount, addRandomDeletes = true)
      val keyValues3 = randomIntKeyValues(keyValuesCount, addRandomDeletes = true)

      val segment1 = TestSegment(keyValues1).get
      val segment2 = TestSegment(keyValues2).get
      val segment3 = TestSegment(keyValues3).get

      val all = Slice((keyValues1 ++ keyValues2 ++ keyValues3).toArray).updateStats
      val slice = SegmentWriter.toSlice(all, 0.1)
      slice.get.size shouldBe all.last.stats.segmentSize

      val readKeyValues = Segment.getAllKeyValues(0.1, Seq(segment1, segment2, segment3)).assertGet

      //ignore stats since keyValues are read from different Segments, their valueOffsets will be different and
      //therefore the segmentSize and other stats will be different.
      readKeyValues shouldBe(all, ignoreStats = true)
    }

    "fail read if reading any one Segment fails for persistent Segments" in {
      val keyValues1 = randomIntKeyValues(keyValuesCount)
      val keyValues2 = randomIntKeyStringValues(keyValuesCount)
      val keyValues3 = randomIntKeyStringValues(keyValuesCount)

      val segment1 = TestSegment(keyValues1).assertGet
      val segment2 = TestSegment(keyValues2).assertGet
      val segment3 = TestSegment(keyValues3).assertGet

      segment3.delete.assertGet //delete a segment so that there is a failure.

      val all = Segment.getAllKeyValues(0.1, Seq(segment1, segment2, segment3))
      all.failed.assertGet shouldBe a[NoSuchFileException]
    }

    "fail read if reading any one Segment file is corrupted" in {
      if (persistent) {
        val keyValues1 = randomIntKeyValues(keyValuesCount)
        val keyValues2 = randomIntKeyStringValues(keyValuesCount)
        val keyValues3 = randomIntKeyStringValues(keyValuesCount)

        val segment1 = TestSegment(keyValues1).assertGet
        val segment2 = TestSegment(keyValues2).assertGet
        val segment3 = TestSegment(keyValues3).assertGet

        val bytes = Files.readAllBytes(segment2.path)

        Files.write(segment2.path, bytes.drop(1))
        Segment.getAllKeyValues(0.1, Seq(segment1, segment2, segment3)).failed.assertGet shouldBe a[SegmentCorruptionException]

        Files.write(segment2.path, bytes.dropRight(1))
        Segment.getAllKeyValues(0.1, Seq(segment2)).failed.assertGet shouldBe a[SegmentCorruptionException]

        Files.write(segment2.path, bytes.drop(10))
        Segment.getAllKeyValues(0.1, Seq(segment1, segment2, segment3)).failed.assertGet shouldBe a[SegmentCorruptionException]

        Files.write(segment2.path, bytes.dropRight(1))
        Segment.getAllKeyValues(0.1, Seq(segment1, segment2, segment3)).failed.assertGet shouldBe a[SegmentCorruptionException]
      } else {
        //memory files do not require this test
      }
    }
  }

  "Segment.delete" should {
    "clear cache, close the channel and delete the file" in {
      val keyValues = randomIntKeyStringValues()
      val segment = TestSegment(keyValues).assertGet
      assertReads(keyValues, segment) //populate the cache

      segment.cacheSize shouldBe keyValues.size

      segment.delete.assertGet

      segment.cacheSize shouldBe 0
      segment.existsOnDisk shouldBe false
      if (persistent) {
        segment.isOpen shouldBe false
        segment.isFileDefined shouldBe false
      }
    }
  }

  "Segment.get" should {
    "fetch a key and value" in {
      val keyValues = randomIntKeyStringValues(100)
      val segment = TestSegment(keyValues).assertGet
      assertGet(keyValues, segment)
    }

    "add unsliced keys and values to the cache for persistent and sliced keys and values for in-memory Segments" in {
      val keyValues = randomIntKeyStringValues(100)
      val segment = TestSegment(keyValues).assertGet

      (0 until keyValues.size) foreach {
        index =>
          val keyValue = keyValues(index)
          if (persistent) segment.getFromCache(keyValue.key) shouldBe empty
          segment.get(keyValue.key).assertGetOpt shouldBe keyValue

          val gotFromCache = eventually(segment.getFromCache(keyValue.key).assertGet)
          //underlying array sizes should not be slices but copied of arrays when the Segment is persistent
          gotFromCache.key.underlyingArraySize shouldBe keyValue.key.toArray.length
          gotFromCache.getOrFetchValue.assertGet.underlyingArraySize shouldBe keyValue.getOrFetchValue.assertGet.toArray.length
      }
    }

    "return None if the key does not exist" in {
      val segment = TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats).assertGet

      segment.get(3).assertGetOpt.isEmpty shouldBe true
    }

    "return None value if the key exists and if its a delete key value" in {
      val keyValues = Slice(Transient.Put(1), Remove(2), Transient.Put(3, 4)).updateStats
      val segment = TestSegment(keyValues).assertGet

      segment.get(2).assertGet shouldBe keyValues(1)
    }

    "add read key values to cache" in {
      val keyValues = randomIntKeyValues(count = 10)
      val segment = TestSegment(keyValues).assertGet

      keyValues foreach {
        keyValue =>
          if (persistent) segment isInCache keyValue.key shouldBe false
          (segment get keyValue.key).assertGet shouldBe keyValue
          eventually(segment isInCache keyValue.key shouldBe true)
      }
    }

    "read value from a closed ValueReader" in {
      val keyValues = Slice(Transient.Put(1, 1), Transient.Put(2, 2)).updateStats
      val segment = TestSegment(keyValues).assertGet

      val keyValue = segment.get(2).assertGet

      segment.close.assertGet

      keyValue.getOrFetchValue.assertGet shouldBe 2

    }

    "lazily load values" in {
      val keyValues = randomIntKeyStringValues(keyValuesCount)
      val segment = TestSegment(keyValues).assertGet

      keyValues foreach {
        keyValue =>
          val readKeyValue = segment.get(keyValue.key).assertGet
          readKeyValue.isValueDefined shouldBe false
          //read the value
          readKeyValue.getOrFetchValue.assertGetOpt shouldBe keyValue.getOrFetchValue.assertGetOpt
          //value is now set
          readKeyValue.isValueDefined shouldBe true
      }
    }
  }

  "Segment.getAll" should {
    "read full index" in {
      val keyValues = randomIntKeyValues(count = 10)
      val segment = TestSegment(keyValues).assertGet

      if (persistent) segment.isCacheEmpty shouldBe true

      val indexEntries = segment.getAll(0.1).assertGet

      (0 until keyValues.size).foreach {
        index =>
          val actualKeyValue = keyValues(index)
          val indexEntryKeyValue = indexEntries(index)

          //ensure that indexEntry's values are not already read as they are lazily fetched from the file.
          indexEntryKeyValue.isValueDefined shouldBe false
          actualKeyValue shouldBe indexEntryKeyValue //after comparison values should be populated.
          indexEntryKeyValue.isValueDefined shouldBe true
      }
    }
  }

  "Segment.lower and Segment.lowerKey" should {
    "get the lower from the segment" in {
      val keyValues = Slice(Transient.Put(1, 1), Remove(2), Transient.Put(3, 3)).updateStats
      val segment = TestSegment(keyValues).assertGet

      segment.lower(0).assertGetOpt shouldBe empty //smallest key in this segment is 1
      segment.lower(1).assertGetOpt shouldBe empty

      val lowerOf2 = segment.lower(2).assertGet
      lowerOf2.key shouldBe 1
      lowerOf2.getOrFetchValue.assertGet shouldBe 1
      lowerOf2.isRemove shouldBe false

      val lowerOf3 = segment.lower(3).assertGet
      lowerOf3.key shouldBe 2
      lowerOf3.getOrFetchValue.assertGetOpt shouldBe None
      lowerOf3.isRemove shouldBe true

      val lowerOf4 = segment.lower(4).assertGet
      lowerOf4.key shouldBe 3
      lowerOf4.getOrFetchValue.assertGet shouldBe 3
      lowerOf4.isRemove shouldBe false
    }

    "get the lower key from the segment that has only 1 key" in {
      val keyValues = Slice(Remove(1)).updateStats
      val segment = TestSegment(keyValues).assertGet

      segment.lower(0).assertGetOpt shouldBe empty //smallest key in this segment is 1
      segment.lower(1).assertGetOpt shouldBe empty

      val lowerOf2 = segment.lower(2).assertGet
      lowerOf2.key shouldBe 1
      lowerOf2.getOrFetchValue.assertGetOpt shouldBe None
      lowerOf2.isRemove shouldBe true
    }

    "get the lower key from the segment that has only many keys key" in {
      val keyValues = randomIntKeyStringValues(keyValuesCount)
      val segment = TestSegment(keyValues).assertGet

      assertLower(keyValues, segment)
    }
  }

  "Segment.higher and Segment.higherKey" should {
    "get the higher from the segment" in {
      val keyValues = Slice(Transient.Put(1, 1), Remove(2), Transient.Put(3, 3)).updateStats
      val segment = TestSegment(keyValues).assertGet

      val higherOf0 = segment.higher(0).assertGet
      higherOf0.key shouldBe 1
      higherOf0.getOrFetchValue.assertGet shouldBe 1
      higherOf0.isRemove shouldBe false

      val higherOf1 = segment.higher(1).assertGet
      higherOf1.key shouldBe 2
      higherOf1.getOrFetchValue.assertGetOpt shouldBe None
      higherOf1.isRemove shouldBe true

      val higherOf2 = segment.higher(2).assertGet
      higherOf2.key shouldBe 3
      higherOf2.getOrFetchValue.assertGet shouldBe 3
      higherOf2.isRemove shouldBe false

      segment.higher(3).assertGetOpt shouldBe empty //smallest key in this segment is 1
      segment.higher(4).assertGetOpt shouldBe empty

    }

    "get the higher key from the segment that has only 1 key" in {
      val keyValues = Slice(Remove(1)).updateStats
      val segment = TestSegment(keyValues).assertGet

      val higherOf0 = segment.higher(0).assertGet
      higherOf0.key shouldBe 1
      higherOf0.getOrFetchValue.assertGetOpt shouldBe None
      higherOf0.isRemove shouldBe true

      segment.higher(1).assertGetOpt shouldBe empty
      segment.higher(2).assertGetOpt shouldBe empty
    }
  }
}
