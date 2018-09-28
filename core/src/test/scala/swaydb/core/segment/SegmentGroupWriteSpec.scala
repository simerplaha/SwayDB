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

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.core.TestBase
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

//@formatter:off
class SegmentGroupWriteSpec0 extends SegmentGroupWriteSpec {
  val keyValuesCount = 100000
}

class SegmentGroupWriteSpec1 extends SegmentGroupWriteSpec {
  val keyValuesCount = 100000
  override def levelFoldersCount = 1
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentGroupWriteSpec2 extends SegmentGroupWriteSpec {
  val keyValuesCount = 100000
  override def inMemoryStorage = true
}
//@formatter:on

sealed trait SegmentGroupWriteSpec extends TestBase with ScalaFutures with PrivateMethodTester {

  def keyValuesCount: Int

  override def deleteFiles = false

  "Deleting all Grouped key-values" should {
    "return empty Segments" in {
      runThis(5.times) {
        val keyValues = randomizedIntKeyValues(keyValuesCount)

        implicit val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = Some(randomCompressionType(keyValuesCount))
        val segment = TestSegment(keyValues).assertGet

        //write same key-values, it should result in merge with grouping.
        val groupedSegments = segment.put(keyValues.toMemory, 10.mb, 0.1, 1.seconds, true).assertGet
        //        printGroupHierarchy(newSegments)
        groupedSegments should have size 1
        val newGroupedSegment = groupedSegments.head
        //perform reads, grouping should result in accurate read results.
        assertReads(keyValues, newGroupedSegment)

        //submit remove key-values either single removes or range removed.
        val removeKeyValues: Slice[Transient] =
          eitherOne(
            left = {
              println("Range Remove used")
              Slice(Memory.Range(0, Int.MaxValue, Some(Value.Remove(None)), Value.Remove(None))).toTransient
            },
            right = {
              println("Fixed Remove used")
              keyValues map {
                keyValue =>
                  Memory.Remove(keyValue.key)
              } toTransient
            }
          )

        //merge remove key-values into the grouped Segment. Remove should return empty.
        val newSegmentsWithRemovedKeyValues =
          newGroupedSegment.put(
            newKeyValues = removeKeyValues,
            minSegmentSize = 10.mb,
            bloomFilterFalsePositiveRate = 0.1,
            hasTimeLeftAtLeast = 10.seconds,
            compressDuplicateValues = true
          ).assertGet

        newSegmentsWithRemovedKeyValues should have size 1
        val lastSegment = newSegmentsWithRemovedKeyValues.head
        keyValues foreach {
          keyValue =>
            lastSegment.get(keyValue.key).assertGet match {
              case _: KeyValue.ReadOnly.Remove =>
              case remove: KeyValue.ReadOnly.Range =>
                remove.fetchFromOrElseRangeValue.assertGet shouldBe Value.Remove(None)
              case actual =>
                fail(s"Expected Remove found ${actual.getClass.getName}")
            }
        }
      }
    }
  }
}

//test code to rest persistent Segment
//      import swaydb.core.TestLimitQueues._
//      implicit val any = (any: Any, any2: Any) => ()
//      implicit val any2 = (any: Any) => ()
//      import scala.concurrent.ExecutionContext.Implicits.global
//
//      val newSegment =
//        Segment(
//          path = Paths.get("/Users/simer/IdeaProjects/SwayDB.range/core/target/TEST_FILES/SegmentGroupWriteSpec0/12/2.seg"),
//          mmapReads = true,
//          mmapWrites = true,
//          minKey = 0,
//          maxKey = MaxKey.Fixed(keyValuesCount),
//          segmentSize = 5.mb,
//          removeDeletes = false,
//          nearestExpiryDeadline = None
//        ).assertGet

//      println(newSegment.path)
