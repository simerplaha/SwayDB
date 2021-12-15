/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.defrag

import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.data.merge.stats.{MergeStats, MergeStatsCreator, MergeStatsSizeCalculator}
import swaydb.core.segment.data.{KeyValue, Memory}
import swaydb.core.segment.ref.SegmentRef
import swaydb.core.segment.{MemorySegment, PersistentSegment, PersistentSegmentMany, Segment}

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

  def run[S >: Null <: MergeStats.Segment[Memory, ListBuffer]](gap: Iterable[Assignable.Gap[S]],
                                                               fragments: ListBuffer[TransientSegment.Fragment[S]],
                                                               removeDeletes: Boolean,
                                                               createdInLevel: Int,
                                                               hasNext: Boolean)(implicit segmentConfig: SegmentBlockConfig,
                                                                                 mergeStatsCreator: MergeStatsCreator[S],
                                                                                 mergeStatsSizeCalculator: MergeStatsSizeCalculator[S]): ListBuffer[TransientSegment.Fragment[S]] = {
    val gapIterator = gap.iterator

    gapIterator.foldLeft(DefragCommon.lastStatsOrNull(fragments)) {
      case (statsOrNull, segment: Segment) =>
        processSegment(
          statsOrNull = statsOrNull,
          fragments = fragments,
          segment = segment,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          //either this iterator hasNext or whatever calling this function hasNext.
          hasNext = gapIterator.hasNext || hasNext
        )

      case (statsOrNull, segmentRef: SegmentRef) =>
        processSegmentRef(
          statsOrNull = statsOrNull,
          fragments = fragments,
          ref = segmentRef,
          removeDeletes = removeDeletes,
          //either this iterator hasNext or whatever calling this function hasNext.
          hasNext = gapIterator.hasNext || hasNext
        )

      case (statsOrNull, collection: Assignable.Collection) =>
        val newOrOldStats =
          if (statsOrNull == null) {
            val newStats = mergeStatsCreator.create(removeDeletes = removeDeletes)
            fragments += TransientSegment.Stats(newStats)
            newStats
          } else {
            statsOrNull
          }

        collection.iterator(segmentConfig.initialiseIteratorsInOneSeek) foreach (keyValue => newOrOldStats.addOne(keyValue.toMemory()))

        newOrOldStats

      case (statsOrNull, Assignable.Stats(stats)) =>
        if (statsOrNull == null) {
          fragments += TransientSegment.Stats(stats)
          stats
        } else {
          statsOrNull addAll stats.keyValues
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

  @inline private def addToStats[S >: Null <: MergeStats.Segment[Memory, ListBuffer]](keyValues: Iterator[KeyValue],
                                                                                      statsOrNull: S,
                                                                                      fragments: ListBuffer[TransientSegment.Fragment[S]],
                                                                                      removeDeletes: Boolean)(implicit mergeStatsCreator: MergeStatsCreator[S]): S =
    if (statsOrNull != null) {
      keyValues foreach (keyValue => statsOrNull.addOne(keyValue.toMemory()))
      statsOrNull
    } else {
      val stats = mergeStatsCreator.create(removeDeletes)
      keyValues foreach (keyValue => stats.addOne(keyValue.toMemory()))
      fragments += TransientSegment.Stats(stats)
      stats
    }

  private def processSegment[S >: Null <: MergeStats.Segment[Memory, ListBuffer]](statsOrNull: S,
                                                                                  fragments: ListBuffer[TransientSegment.Fragment[S]],
                                                                                  segment: Segment,
                                                                                  removeDeletes: Boolean,
                                                                                  createdInLevel: Int,
                                                                                  hasNext: Boolean)(implicit segmentConfig: SegmentBlockConfig,
                                                                                                    mergeStatsCreator: MergeStatsCreator[S],
                                                                                                    mergeStatsSizeCalculator: MergeStatsSizeCalculator[S]): S =
    if ((hasNext && DefragCommon.isSegmentSmall(segment)) || mergeStatsSizeCalculator.isStatsOrNullSmall(statsOrNull))
      segment match {
        case many: PersistentSegmentMany =>
          val refIterator = many.segmentRefs(segmentConfig.initialiseIteratorsInOneSeek)

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
            keyValues = segment.iterator(segmentConfig.initialiseIteratorsInOneSeek),
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

  private def processSegmentRef[S >: Null <: MergeStats.Segment[Memory, ListBuffer]](statsOrNull: S,
                                                                                     fragments: ListBuffer[TransientSegment.Fragment[S]],
                                                                                     ref: SegmentRef,
                                                                                     removeDeletes: Boolean,
                                                                                     hasNext: Boolean)(implicit segmentConfig: SegmentBlockConfig,
                                                                                                       mergeStatsCreator: MergeStatsCreator[S],
                                                                                                       mergeStatsSizeCalculator: MergeStatsSizeCalculator[S]): S =
    if ((hasNext && DefragCommon.isSegmentRefSmall(ref)) || mergeStatsSizeCalculator.isStatsOrNullSmall(statsOrNull))
      addToStats(
        keyValues = ref.iterator(segmentConfig.initialiseIteratorsInOneSeek),
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

  private def addRemoteSegment[S >: Null <: MergeStats.Segment[Memory, ListBuffer]](segment: Segment,
                                                                                    statsOrNull: S,
                                                                                    fragments: ListBuffer[TransientSegment.Fragment[S]],
                                                                                    removeDeletes: Boolean,
                                                                                    createdInLevel: Int)(implicit mergeStatsCreator: MergeStatsCreator[S],
                                                                                                         segmentConfig: SegmentBlockConfig): S =
    if (removeDeletes && segment.hasUpdateOrRangeOrExpired())
      segment match {
        case segment: PersistentSegmentMany =>
          segment.segmentRefs(segmentConfig.initialiseIteratorsInOneSeek).foldLeft(statsOrNull) {
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
            keyValues = segment.iterator(segmentConfig.initialiseIteratorsInOneSeek),
            statsOrNull = statsOrNull,
            fragments = fragments,
            removeDeletes = removeDeletes
          )
      }
    else
      segment match {
        case segment: MemorySegment =>
          addToStats(
            keyValues = segment.iterator(segmentConfig.initialiseIteratorsInOneSeek),
            statsOrNull = statsOrNull,
            fragments = fragments,
            removeDeletes = removeDeletes
          )

        case segment: PersistentSegment =>
          fragments += TransientSegment.RemotePersistentSegment(segment = segment)
          null
      }

  private def addRemoteSegmentRef[S >: Null <: MergeStats.Segment[Memory, ListBuffer]](ref: SegmentRef,
                                                                                       fragments: ListBuffer[TransientSegment.Fragment[S]],
                                                                                       lastMergeStatsOrNull: S,
                                                                                       removeDeletes: Boolean)(implicit mergeStatsCreator: MergeStatsCreator[S],
                                                                                                               segmentConfig: SegmentBlockConfig): S =
    if (removeDeletes && ref.hasUpdateOrRangeOrExpired()) {
      addToStats(
        keyValues = ref.iterator(segmentConfig.initialiseIteratorsInOneSeek),
        statsOrNull = lastMergeStatsOrNull,
        fragments = fragments,
        removeDeletes = removeDeletes
      )
    } else {
      fragments += TransientSegment.RemoteRef(ref)
      null
    }
}
