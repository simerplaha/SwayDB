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

package swaydb.core.segment.merge

import swaydb.core.TestBase
import swaydb.core.data.{KeyValue, Persistent}
import swaydb.core.group.compression.data.GroupGroupingStrategyInternal
import swaydb.core.segment.format.one.SegmentWriter
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

import scala.collection.mutable.ListBuffer

class SegmentGrouper_GroupGroups_Count_Spec extends SegmentGrouper_GroupGroups_Spec {
  val useCount = true
  val force = false
}

class SegmentGrouper_GroupGroups_Count_Force_Spec extends SegmentGrouper_GroupGroups_Spec {
  val useCount = true
  val force = true
}

class SegmentGrouper_GroupGroups_Size_Spec extends SegmentGrouper_GroupGroups_Spec {
  val useCount = false
  val force = false
}

class SegmentGrouper_GroupGroups_Size_Force_Spec extends SegmentGrouper_GroupGroups_Spec {
  val useCount = false
  val force = false
}

sealed trait SegmentGrouper_GroupGroups_Spec extends TestBase {

  val keyValueCount = 100

  def useCount: Boolean

  def force: Boolean

  "groupGroups" should {
    "return None (Failure to group)" when {
      "there are no key-values" in {
        SegmentGrouper.groupGroups(
          groupKeyValues = ListBuffer.empty,
          bloomFilterFalsePositiveRate = 0.1,
          force = force,
          groupingStrategy =
            if (useCount)
              GroupGroupingStrategyInternal.Count(
                count = 20,
                indexCompression = randomCompression(),
                valueCompression = randomCompression()
              )
            else
              GroupGroupingStrategyInternal.Size(
                size = 0.byte,
                indexCompression = randomCompression(),
                valueCompression = randomCompression()
              )
        ).assertGetOpt shouldBe empty
      }

      "there are not enough Groups" in {
        val group1 = randomGroup(randomizedIntKeyValues(keyValueCount))
        val group2 = randomGroup(randomizedIntKeyValues(keyValueCount, startId = Some(group1.maxKey.maxKey.readInt() + 1)))
        val group3 = randomGroup(randomizedIntKeyValues(keyValueCount, startId = Some(group2.maxKey.maxKey.readInt() + 1)))

        val otherKeyValues = randomizedIntKeyValues(20, startId = Some(group3.maxKey.maxKey.readInt() + 1), addRandomGroups = false)

        val groups = Seq(group1, group2, group3).updateStats
        val keyValues = (groups ++ otherKeyValues).updateStats
        val mutableKeyValues = ListBuffer(keyValues.toList: _*)

        val result =
          SegmentGrouper.groupGroups(
            groupKeyValues = mutableKeyValues,
            bloomFilterFalsePositiveRate = 0.1,
            force = force,
            groupingStrategy =
              if (useCount)
                GroupGroupingStrategyInternal.Count(
                  count = 4,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
              else
                GroupGroupingStrategyInternal.Size(
                  size = groups.last.stats.segmentSizeWithoutFooter + 10000,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
          )

        if (!force) {
          result.assertGetOpt shouldBe empty
          //no mutation occurs
          mutableKeyValues shouldBe keyValues
        } else {
          val (bytes, _) = SegmentWriter.write(Slice(result.assertGet), 0.1).assertGet
          val rootGroup = readAll(bytes).assertGet
          rootGroup should have size 1
          rootGroup.head.asInstanceOf[Persistent.Group].segmentCache.getAll().assertGet shouldBe keyValues
        }
      }

      "there are enough Groups but compression's minimum requirement is not satisfied" in {
        val group1 = randomGroup(randomizedIntKeyValues(keyValueCount))
        val group2 = randomGroup(randomizedIntKeyValues(keyValueCount, startId = Some(group1.maxKey.maxKey.readInt() + 1)))
        val group3 = randomGroup(randomizedIntKeyValues(keyValueCount, startId = Some(group2.maxKey.maxKey.readInt() + 1)))

        val groups = Seq(group1, group2, group3).updateStats
        val mutableKeyValues = ListBuffer(groups.toList: _*)

        SegmentGrouper.groupGroups(
          groupKeyValues = mutableKeyValues,
          bloomFilterFalsePositiveRate = 0.1,
          force = force,
          groupingStrategy =
            if (useCount)
              GroupGroupingStrategyInternal.Count(
                count = 3,
                indexCompression = randomCompressionLZ4OrSnappy(100),
                valueCompression = randomCompression()
              )
            else
              GroupGroupingStrategyInternal.Size(
                size = groups.last.stats.segmentSizeWithoutFooter,
                indexCompression = randomCompressionLZ4OrSnappy(100),
                valueCompression = randomCompression()
              )
        ).assertGetOpt shouldBe empty

        //no mutation occurs
        mutableKeyValues shouldBe groups
      }

      "a Group exists" in {
        Seq(1, 10, 100) foreach {
          minCount =>
            val group = randomGroup()
            val mutableKeyValues = ListBuffer(group: KeyValue.WriteOnly)

            SegmentGrouper.groupGroups(
              groupKeyValues = mutableKeyValues,
              bloomFilterFalsePositiveRate = 0.1,
              force = force,
              groupingStrategy =
                if (useCount)
                  GroupGroupingStrategyInternal.Count(
                    count = minCount,
                    indexCompression = randomCompression(),
                    valueCompression = randomCompression()
                  )
                else
                  GroupGroupingStrategyInternal.Size(
                    size = minCount,
                    indexCompression = randomCompression(),
                    valueCompression = randomCompression()
                  )
            ).assertGetOpt shouldBe empty

            //no mutation occurs
            mutableKeyValues should have size 1
            mutableKeyValues.head shouldBe group
        }
      }

      "multiple Groups exists but the limit is not reached" in {
        val group1 = randomGroup(randomizedIntKeyValues(keyValueCount))
        val group2 = randomGroup(randomizedIntKeyValues(keyValueCount, startId = Some(group1.maxKey.maxKey.readInt() + 1)))
        val group3 = randomGroup(randomizedIntKeyValues(keyValueCount, startId = Some(group2.maxKey.maxKey.readInt() + 1)))

        val groups = Seq(group1, group2, group3).updateStats

        Seq(4, 10, 100) foreach {
          minCount =>
            val mutableKeyValues = ListBuffer(groups.toList: _*)

            val result =
              SegmentGrouper.groupGroups(
                groupKeyValues = mutableKeyValues,
                bloomFilterFalsePositiveRate = 0.1,
                force = force,
                groupingStrategy =
                  if (useCount)
                    GroupGroupingStrategyInternal.Count(
                      count = minCount,
                      indexCompression = randomCompression(),
                      valueCompression = randomCompression()
                    )
                  else
                    GroupGroupingStrategyInternal.Size(
                      size = mutableKeyValues.last.stats.segmentSizeWithoutFooter + 1,
                      indexCompression = randomCompression(),
                      valueCompression = randomCompression()
                    )
              )
            if (!force) {
              result.assertGetOpt shouldBe empty
              //no mutation occurs
              mutableKeyValues shouldBe groups
            } else {
              val (bytes, _) = SegmentWriter.write(Slice(result.assertGet).updateStats, 0.1).assertGet
              val rootGroup = readAll(bytes).assertGet
              rootGroup should have size 1
              rootGroup.head.asInstanceOf[Persistent.Group].segmentCache.getAll().assertGet shouldBe groups
            }
        }
      }
    }

    "return Compressed group (min compression requirement is satisfied - Successfully grouped)" when {
      "there are key-values" in {
        val group1 = randomGroup(randomizedIntKeyValues(keyValueCount))
        val group2 = randomGroup(randomizedIntKeyValues(keyValueCount, startId = Some(group1.maxKey.maxKey.readInt() + 1)))
        val group3 = randomGroup(randomizedIntKeyValues(keyValueCount, startId = Some(group2.maxKey.maxKey.readInt() + 1)))

        val groups = Seq(group1, group2, group3).updateStats
        val mutableKeyValues = ListBuffer(groups.toList: _*)

        val result =
          SegmentGrouper.groupGroups(
            groupKeyValues = mutableKeyValues,
            bloomFilterFalsePositiveRate = 0.1,
            force = force,
            groupingStrategy =
              if (useCount)
                GroupGroupingStrategyInternal.Count(
                  count = groups.size,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
              else
                GroupGroupingStrategyInternal.Size(
                  size = groups.last.stats.segmentSizeWithoutFooter,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
          )

        //all key-values are merged into one group.
        mutableKeyValues should have size 1
        //only a Group key-value exists with
        val (bytes, _) = SegmentWriter.write(Slice(result.assertGet).updateStats, 0.1).assertGet
        val rootGroup = readAll(bytes).assertGet
        rootGroup should have size 1
        rootGroup.head.asInstanceOf[Persistent.Group].segmentCache.getAll().assertGet shouldBe groups
      }
    }
  }
}