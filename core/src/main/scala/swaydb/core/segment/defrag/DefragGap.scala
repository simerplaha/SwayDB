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

import swaydb.core.data.Memory
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
import swaydb.core.segment.{PersistentSegmentMany, PersistentSegmentOne, Segment}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

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
          createdInLevel: Int)(implicit ec: ExecutionContext,
                               valuesConfig: ValuesBlock.Config,
                               sortedIndexConfig: SortedIndexBlock.Config,
                               binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                               hashIndexConfig: HashIndexBlock.Config,
                               bloomFilterConfig: BloomFilterBlock.Config,
                               segmentConfig: SegmentBlock.Config): ListBuffer[TransientSegment.Fragment] = {
    gap.foldLeft(lastStatsOrNull(fragments)) {
      case (statsOrNull, segment: Segment) =>
        processSegment(
          statsOrNull = statsOrNull,
          fragments = fragments,
          segment = segment,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel
        )

      case (statsOrNull, segmentRef: SegmentRef) =>
        processSegmentRef(
          statsOrNull = statsOrNull,
          fragments = fragments,
          segmentRef = segmentRef,
          removeDeletes = removeDeletes,
          sortedIndexConfig = sortedIndexConfig,
          segmentConfig = segmentConfig
        )

      case (statsOrNull, collection: Assignable.Collection) =>
        val stats =
          if (statsOrNull == null) {
            val newStats = DefragCommon.createMergeStats(removeDeletes = removeDeletes)
            fragments += TransientSegment.Stats(newStats)
            newStats
          } else {
            statsOrNull
          }

        collection.iterator() foreach (keyValue => stats.add(keyValue.toMemory()))

        stats

      case (statsOrNull, Assignable.Stats(stats)) =>
        if (statsOrNull == null) {
          fragments += TransientSegment.Stats(stats)
          stats
        } else {
          stats.keyValues foreach statsOrNull.add
          statsOrNull
        }
    }

    fragments
  }

  @inline def lastStatsOrNull(fragments: ListBuffer[TransientSegment.Fragment]): MergeStats.Persistent.Builder[Memory, ListBuffer] =
    fragments.lastOption match {
      case Some(stats: TransientSegment.Stats) =>
        stats.stats

      case Some(_) | None =>
        null
    }

  def addRemoteSegment(segment: Segment,
                       lastMergeStatsOrNull: MergeStats.Persistent.Builder[Memory, ListBuffer],
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
          segment.segmentRefsIterator().foldLeft(lastMergeStatsOrNull) {
            case (lastMergeStatsOrNull, ref) =>
              addRemoteSegmentRef(
                ref = ref,
                fragments = fragments,
                lastMergeStatsOrNull = lastMergeStatsOrNull,
                removeDeletes = removeDeletes
              )
          }

        case _ =>
          if (lastMergeStatsOrNull != null) {
            segment.iterator() foreach (keyValue => lastMergeStatsOrNull.add(keyValue.toMemory()))
            lastMergeStatsOrNull
          } else {
            val stats = DefragCommon.createMergeStats(removeDeletes)
            segment.iterator() foreach (keyValue => stats.add(keyValue.toMemory()))
            fragments += TransientSegment.Stats(stats)
            stats
          }
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

  def addRemoteSegmentRef(ref: SegmentRef,
                          fragments: ListBuffer[TransientSegment.Fragment],
                          lastMergeStatsOrNull: MergeStats.Persistent.Builder[Memory, ListBuffer],
                          removeDeletes: Boolean): MergeStats.Persistent.Builder[Memory, ListBuffer] =
    if (removeDeletes && ref.hasUpdateOrRange) {
      if (lastMergeStatsOrNull != null) {
        ref.iterator() foreach (keyValue => lastMergeStatsOrNull.add(keyValue.toMemory()))
        lastMergeStatsOrNull
      } else {
        val stats = DefragCommon.createMergeStats(removeDeletes)
        ref.iterator() foreach (keyValue => stats.add(keyValue.toMemory()))
        fragments += TransientSegment.Stats(stats)
        stats
      }
    } else {
      val remote =
        TransientSegment.RemoteRef(
          fileHeader = PersistentSegmentOne.formatIdSlice,
          ref = ref
        )

      fragments += remote
      null
    }

  def processSegment(statsOrNull: MergeStats.Persistent.Builder[Memory, ListBuffer],
                     fragments: ListBuffer[TransientSegment.Fragment],
                     segment: Segment,
                     removeDeletes: Boolean,
                     createdInLevel: Int)(implicit valuesConfig: ValuesBlock.Config,
                                          sortedIndexConfig: SortedIndexBlock.Config,
                                          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                          hashIndexConfig: HashIndexBlock.Config,
                                          bloomFilterConfig: BloomFilterBlock.Config,
                                          segmentConfig: SegmentBlock.Config) =
    if (statsOrNull == null) //does matter if this Segment is small. Add it because there are currently no known opened stats.
      addRemoteSegment(
        segment = segment,
        lastMergeStatsOrNull = statsOrNull,
        fragments = fragments,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel
      )
    else if (segment.segmentSize < segmentConfig.minSize || DefragCommon.isStatsSmall(statsOrNull, sortedIndexConfig, segmentConfig))
      segment match {
        case many: PersistentSegmentMany =>
          many.segmentRefsIterator().foldLeft(statsOrNull) {
            case (statsOrNull, segmentRef) =>
              processSegmentRef(
                statsOrNull = statsOrNull,
                fragments = fragments,
                segmentRef = segmentRef,
                removeDeletes = removeDeletes,
                sortedIndexConfig = sortedIndexConfig,
                segmentConfig = segmentConfig
              )
          }

        case _ =>
          segment.iterator() foreach (keyValue => statsOrNull.add(keyValue.toMemory()))
          statsOrNull
      }
    else
      addRemoteSegment(
        segment = segment,
        lastMergeStatsOrNull = statsOrNull,
        fragments = fragments,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel
      )

  def processSegmentRef(statsOrNull: MergeStats.Persistent.Builder[Memory, ListBuffer],
                        fragments: ListBuffer[TransientSegment.Fragment],
                        segmentRef: SegmentRef,
                        removeDeletes: Boolean,
                        sortedIndexConfig: SortedIndexBlock.Config,
                        segmentConfig: SegmentBlock.Config) =
    if (statsOrNull == null) {
      //does matter if this Segment is small. Add it because there are currently no known opened stats.
      addRemoteSegmentRef(
        ref = segmentRef,
        fragments = fragments,
        lastMergeStatsOrNull = statsOrNull,
        removeDeletes = removeDeletes
      )
    } else if (segmentRef.keyValueCount < segmentConfig.maxCount || DefragCommon.isStatsSmall(statsOrNull, sortedIndexConfig, segmentConfig)) {
      segmentRef.iterator() foreach (keyValue => statsOrNull.add(keyValue.toMemory()))
      statsOrNull
    } else {
      addRemoteSegmentRef(
        ref = segmentRef,
        fragments = fragments,
        lastMergeStatsOrNull = statsOrNull,
        removeDeletes = removeDeletes
      )
    }
}
