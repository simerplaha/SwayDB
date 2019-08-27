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

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.core.CommonAssertions._
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.group.compression.GroupByInternal
import swaydb.core.segment.format.a.block.SegmentBlock
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

class SegmentGroupWriteSpec0 extends SegmentGroupWriteSpec {
  val keyValuesCount = 1000
}

class SegmentGroupWriteSpec1 extends SegmentGroupWriteSpec {
  val keyValuesCount = 1000
  override def levelFoldersCount = 1
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentGroupWriteSpec2 extends SegmentGroupWriteSpec {
  val keyValuesCount = 1000
  override def inMemoryStorage = true
}

sealed trait SegmentGroupWriteSpec extends TestBase with ScalaFutures with PrivateMethodTester {

  def keyValuesCount: Int

  implicit def testTimer: TestTimer = TestTimer.Empty

  "Deleting all Grouped key-values" should {
    "return empty Segments" in {
      runThis(20.times, log = true) {
        val rightKeyValues = randomizedKeyValues(keyValuesCount)
        //add another head key-value that is used to a merge split to occur.
        val mergePut = randomPutKeyValue(rightKeyValues.head.key.readInt() - 1).toTransient

        //all key-values to remove and assert
        val keyValues = (Slice(mergePut) ++ rightKeyValues).updateStats

        implicit val groupBy: Option[GroupByInternal.KeyValues] = Some(randomGroupBy(keyValuesCount))
        val segment = TestSegment(keyValues).right.value

        //write a head key-values so that it triggers merging and grouping
        val groupedSegments =
          segment.put(
            newKeyValues = Slice(mergePut.toMemory),
            minSegmentSize = 10.mb,
            removeDeletes = false,
            createdInLevel = 0,
            valuesConfig = keyValues.last.valuesConfig,
            sortedIndexConfig = keyValues.last.sortedIndexConfig,
            binarySearchIndexConfig = keyValues.last.binarySearchIndexConfig,
            hashIndexConfig = keyValues.last.hashIndexConfig,
            bloomFilterConfig = keyValues.last.bloomFilterConfig,
            segmentConfig = SegmentBlock.Config.random
          ).runRandomIO.right.value
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
              Slice(Memory.Range(0, Int.MaxValue, Some(Value.remove(None)), Value.remove(None))).toTransient
            },
            right = {
              println("Fixed Remove used")
              keyValues map {
                keyValue =>
                  Memory.remove(keyValue.key)
              } toTransient
            }
          )

        //merge remove key-values into the grouped Segment. Remove should return empty.
        val newSegmentsWithRemovedKeyValues =
          newGroupedSegment.put(
            newKeyValues = removeKeyValues.toMemory,
            minSegmentSize = 10.mb,
            removeDeletes = false,
            createdInLevel = 0,
            valuesConfig = keyValues.last.valuesConfig,
            sortedIndexConfig = keyValues.last.sortedIndexConfig,
            binarySearchIndexConfig = keyValues.last.binarySearchIndexConfig,
            hashIndexConfig = keyValues.last.hashIndexConfig,
            bloomFilterConfig = keyValues.last.bloomFilterConfig,
            segmentConfig = SegmentBlock.Config.random
          ).runRandomIO.right.value

        newSegmentsWithRemovedKeyValues should have size 1
        val lastSegment = newSegmentsWithRemovedKeyValues.head
        keyValues foreach {
          keyValue =>
            lastSegment.get(keyValue.key).runRandomIO.right.value.get match {
              case _: KeyValue.ReadOnly.Remove =>
              case remove: KeyValue.ReadOnly.Range =>
                remove.fetchFromOrElseRangeValue.runRandomIO.right.value shouldBe Value.remove(None)
              case actual =>
                fail(s"Expected Remove found ${actual.getClass.getName}")
            }
        }
      }
    }
  }
}
