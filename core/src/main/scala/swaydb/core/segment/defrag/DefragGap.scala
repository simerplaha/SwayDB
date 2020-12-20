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

package swaydb.core.segment.defrag

import swaydb.core.data.{KeyValue, Memory}
import swaydb.core.merge.MergeStats
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.ref.SegmentRef
import swaydb.core.segment.{PersistentSegmentMany, Segment}

import scala.collection.mutable.ListBuffer

/**
 * Defrag gap key-values or [[Assignable.Collection]] by avoiding expanding collections as much as possible
 * so that we can defer transfer bytes to OS skipping JVM heap allocation.
 *
 * Always expand if
 *  - the collection has removable/cleanable key-values.
 *  - the collection is small or head key-values are too small.
 */

private[segment] object DefragGap {

  def run(gap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          fragments: ListBuffer[TransientSegment.Fragment],
          removeDeletes: Boolean,
          createdInLevel: Int,
          hasNext: Boolean)(implicit valuesConfig: ValuesBlock.Config,
                            sortedIndexConfig: SortedIndexBlock.Config,
                            binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                            hashIndexConfig: HashIndexBlock.Config,
                            bloomFilterConfig: BloomFilterBlock.Config,
                            segmentConfig: SegmentBlock.Config): ListBuffer[TransientSegment.Fragment] = {
    val iterator = gap.iterator

    iterator.foldLeft(DefragCommon.lastStatsOrNull(fragments)) {
      case (statsOrNull, segment: Segment) =>
        processSegment(
          statsOrNull = statsOrNull,
          fragments = fragments,
          segment = segment,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          //either this iterator hasNext or whatever calling this function hasNext.
          hasNext = iterator.hasNext || hasNext
        )

      case (statsOrNull, segmentRef: SegmentRef) =>
        processSegmentRef(
          statsOrNull = statsOrNull,
          fragments = fragments,
          ref = segmentRef,
          removeDeletes = removeDeletes,
          //either this iterator hasNext or whatever calling this function hasNext.
          hasNext = iterator.hasNext || hasNext
        )

      case (statsOrNull, collection: Assignable.Collection) =>
        val newOrOldStats =
          if (statsOrNull == null) {
            val newStats = DefragCommon.createMergeStats(removeDeletes = removeDeletes)
            fragments += TransientSegment.Stats(newStats)
            newStats
          } else {
            statsOrNull
          }

        collection.iterator() foreach (keyValue => newOrOldStats.add(keyValue.toMemory()))

        newOrOldStats

      case (statsOrNull, Assignable.Stats(stats)) =>
        if (statsOrNull == null) {
          fragments += TransientSegment.Stats(stats)
          stats
        } else {
          stats.keyValues foreach statsOrNull.add
          statsOrNull
        }
    }

    //clear out any empty stats
    fragments filter {
      case TransientSegment.Stats(stats) =>
        !stats.isEmpty

      case _ =>
        true
    }
  }

  @inline private def addToStats(keyValues: Iterator[KeyValue],
                                 statsOrNull: MergeStats.Persistent.Builder[Memory, ListBuffer],
                                 fragments: ListBuffer[TransientSegment.Fragment],
                                 removeDeletes: Boolean): MergeStats.Persistent.Builder[Memory, ListBuffer] =
    if (statsOrNull != null) {
      keyValues foreach (keyValue => statsOrNull.add(keyValue.toMemory()))
      statsOrNull
    } else {
      val stats = DefragCommon.createMergeStats(removeDeletes)
      keyValues foreach (keyValue => stats.add(keyValue.toMemory()))
      fragments += TransientSegment.Stats(stats)
      stats
    }

  private def processSegment(statsOrNull: MergeStats.Persistent.Builder[Memory, ListBuffer],
                             fragments: ListBuffer[TransientSegment.Fragment],
                             segment: Segment,
                             removeDeletes: Boolean,
                             createdInLevel: Int,
                             hasNext: Boolean)(implicit valuesConfig: ValuesBlock.Config,
                                               sortedIndexConfig: SortedIndexBlock.Config,
                                               binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                               hashIndexConfig: HashIndexBlock.Config,
                                               bloomFilterConfig: BloomFilterBlock.Config,
                                               segmentConfig: SegmentBlock.Config): MergeStats.Persistent.Builder[Memory, ListBuffer] =
    if ((hasNext && DefragCommon.isSegmentSmall(segment)) || DefragCommon.isStatsOrNullSmall(statsOrNull))
      segment match {
        case many: PersistentSegmentMany =>
          val refIterator = many.segmentRefsIterator()

          refIterator.foldLeft(statsOrNull) {
            case (statsOrNull, segmentRef) =>
              processSegmentRef(
                statsOrNull = statsOrNull,
                fragments = fragments,
                ref = segmentRef,
                removeDeletes = removeDeletes,
                hasNext = refIterator.hasNext || hasNext
              )
          }

        case _ =>
          addToStats(
            keyValues = segment.iterator(),
            statsOrNull = statsOrNull,
            fragments = fragments,
            removeDeletes = removeDeletes
          )
      }
    else
      addRemoteSegment(
        segment = segment,
        statsOrNull = statsOrNull,
        fragments = fragments,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel
      )

  private def processSegmentRef(statsOrNull: MergeStats.Persistent.Builder[Memory, ListBuffer],
                                fragments: ListBuffer[TransientSegment.Fragment],
                                ref: SegmentRef,
                                removeDeletes: Boolean,
                                hasNext: Boolean)(implicit sortedIndexConfig: SortedIndexBlock.Config,
                                                  segmentConfig: SegmentBlock.Config): MergeStats.Persistent.Builder[Memory, ListBuffer] =
    if ((hasNext && DefragCommon.isSegmentRefSmall(ref)) || DefragCommon.isStatsOrNullSmall(statsOrNull))
      addToStats(
        keyValues = ref.iterator(),
        statsOrNull = statsOrNull,
        fragments = fragments,
        removeDeletes = removeDeletes
      )
    else
      addRemoteSegmentRef(
        ref = ref,
        fragments = fragments,
        lastMergeStatsOrNull = statsOrNull,
        removeDeletes = removeDeletes
      )

  private def addRemoteSegment(segment: Segment,
                               statsOrNull: MergeStats.Persistent.Builder[Memory, ListBuffer],
                               fragments: ListBuffer[TransientSegment.Fragment],
                               removeDeletes: Boolean,
                               createdInLevel: Int)(implicit valuesConfig: ValuesBlock.Config,
                                                    sortedIndexConfig: SortedIndexBlock.Config,
                                                    binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                                    hashIndexConfig: HashIndexBlock.Config,
                                                    bloomFilterConfig: BloomFilterBlock.Config,
                                                    segmentConfig: SegmentBlock.Config): MergeStats.Persistent.Builder[Memory, ListBuffer] =
    if (removeDeletes && segment.hasUpdateOrRange) {
      segment match {
        case segment: PersistentSegmentMany =>
          segment.segmentRefsIterator().foldLeft(statsOrNull) {
            case (lastMergeStatsOrNull, ref) =>
              addRemoteSegmentRef(
                ref = ref,
                fragments = fragments,
                lastMergeStatsOrNull = lastMergeStatsOrNull,
                removeDeletes = removeDeletes
              )
          }

        case _ =>
          addToStats(
            keyValues = segment.iterator(),
            statsOrNull = statsOrNull,
            fragments = fragments,
            removeDeletes = removeDeletes
          )
      }
    } else {
      val remote =
        TransientSegment.RemoteSegment(
          segment = segment,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig
        )

      fragments += remote
      null
    }

  private def addRemoteSegmentRef(ref: SegmentRef,
                                  fragments: ListBuffer[TransientSegment.Fragment],
                                  lastMergeStatsOrNull: MergeStats.Persistent.Builder[Memory, ListBuffer],
                                  removeDeletes: Boolean): MergeStats.Persistent.Builder[Memory, ListBuffer] =
    if (removeDeletes && ref.hasUpdateOrRange) {
      addToStats(
        keyValues = ref.iterator(),
        statsOrNull = lastMergeStatsOrNull,
        fragments = fragments,
        removeDeletes = removeDeletes
      )
    } else {
      fragments += TransientSegment.RemoteRef(ref)
      null
    }
}
