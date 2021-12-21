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

package swaydb.core.segment

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ExceptionHandler
import swaydb.config.{MMAP, SegmentRefCacheLife}
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.file.sweeper.{FileSweeper, FileSweeperItem}
import swaydb.core.file.{CoreFile, ForceSaveApplier}
import swaydb.core.segment.assigner.{Assignable, Assigner}
import swaydb.core.segment.block.BlockCompressionInfo
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockConfig}
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockConfig}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockConfig}
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.core.segment.data._
import swaydb.core.segment.data.merge.KeyValueGrouper
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.io.{SegmentReadIO, SegmentWritePersistentIO}
import swaydb.core.segment.ref.SegmentRef
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.skiplist.{SkipList, SkipListTreeMap}
import swaydb.core.util._
import swaydb.effect.Effect
import swaydb.SliceIOImplicits._
import swaydb.core.segment.distributor.Distributable
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.{MaxKey, Slice, SliceOption}
import swaydb.utils.Collections._
import swaydb.utils.Futures.FutureUnitImplicits
import swaydb.utils.{Aggregator, FiniteDurations, IDGenerator, SomeOrNone}

import java.nio.file.Path
import scala.annotation.tailrec
import scala.collection.compat.IterableOnce
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

private[swaydb] sealed trait SegmentOption extends SomeOrNone[SegmentOption, Segment] {
  override def noneS: SegmentOption =
    Segment.Null
}

private[core] case object Segment extends LazyLogging {

  final case object Null extends SegmentOption {
    override def isNoneS: Boolean = true

    override def getS: Segment = throw new Exception("Segment is of type Null")
  }

  def segmentSizeForMerge(segment: Segment,
                          initialiseIteratorsInOneSeek: Boolean): Int =
    segment match {
      case segment: MemorySegment =>
        segment.segmentSize

      case segment: PersistentSegmentOne =>
        segmentSizeForMerge(segment.ref)

      case segment: PersistentSegmentMany =>
        val listSegmentSize = segmentSizeForMerge(segment.listSegmentCache.getOrFetch(()))

        //1+ for formatId
        segment.segmentRefs(initialiseIteratorsInOneSeek).foldLeft(1 + listSegmentSize) {
          case (size, ref) =>
            size + segmentSizeForMerge(ref)
        }
    }

  def segmentSizeForMerge(segment: SegmentRef): Int = {
    val footer = segment.getFooter()
    footer.sortedIndexOffset.size +
      footer.valuesOffset.map(_.size).getOrElse(0)
  }

  def keyOverlaps(keyValue: KeyValue,
                  segment: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    keyOverlaps(
      keyValue = keyValue,
      minKey = segment.minKey,
      maxKey = segment.maxKey
    )

  def keyOverlaps(keyValue: KeyValue,
                  minKey: Slice[Byte],
                  maxKey: MaxKey[Slice[Byte]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
    import keyOrder._
    keyValue.key >= minKey && {
      if (maxKey.inclusive)
        keyValue.key <= maxKey.maxKey
      else
        keyValue.key < maxKey.maxKey
    }
  }

  def overlaps(assignable: Assignable,
               minKey: Slice[Byte],
               maxKey: MaxKey[Slice[Byte]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    assignable match {
      case keyValue: KeyValue =>
        keyOverlaps(
          keyValue = keyValue,
          minKey = minKey,
          maxKey = maxKey
        )

      case collection: Assignable.Collection =>
        overlaps(
          minKey = collection.key,
          maxKey = collection.maxKey.maxKey,
          maxKeyInclusive = collection.maxKey.inclusive,
          targetMinKey = minKey,
          targetMaxKey = maxKey
        )
    }

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               maxKeyInclusive: Boolean,
               segment: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    overlaps(
      minKey = minKey,
      maxKey = maxKey,
      maxKeyInclusive = maxKeyInclusive,
      targetMinKey = segment.minKey,
      targetMaxKey = segment.maxKey
    )

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               maxKeyInclusive: Boolean,
               targetMinKey: Slice[Byte],
               targetMaxKey: MaxKey[Slice[Byte]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Slice.intersects((minKey, maxKey, maxKeyInclusive), (targetMinKey, targetMaxKey.maxKey, targetMaxKey.inclusive))

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               maxKeyInclusive: Boolean,
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    segments.exists(segment => overlaps(minKey, maxKey, maxKeyInclusive, segment))

  def overlaps(keyValues: Either[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], Slice[Memory]],
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    keyValues match {
      case util.Left(value) =>
        overlaps(value, segments)

      case util.Right(value) =>
        overlaps(value, segments)
    }

  def overlaps(keyValues: Slice[Memory],
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Segment.minMaxKey(keyValues) exists {
      case (minKey, maxKey, maxKeyInclusive) =>
        Segment.overlaps(
          minKey = minKey,
          maxKey = maxKey,
          maxKeyInclusive = maxKeyInclusive,
          segments = segments
        )
    }

  def overlaps(map: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Segment.minMaxKey(map) exists {
      case (minKey, maxKey, maxKeyInclusive) =>
        Segment.overlaps(
          minKey = minKey,
          maxKey = maxKey,
          maxKeyInclusive = maxKeyInclusive,
          segments = segments
        )
    }

  def overlaps(segment1: Assignable.Collection,
               segment2: Assignable.Collection)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Slice.intersects(
      range1 = (segment1.key, segment1.maxKey.maxKey, segment1.maxKey.inclusive),
      range2 = (segment2.key, segment2.maxKey.maxKey, segment2.maxKey.inclusive)
    )

  def partitionOverlapping[A <: Assignable.Collection](segments1: Iterable[A],
                                                       segments2: Iterable[A])(implicit keyOrder: KeyOrder[Slice[Byte]]): (Iterable[A], Iterable[A]) =
    segments1.partition(segmentToWrite => segments2.exists(existingSegment => Segment.overlaps(segmentToWrite, existingSegment)))

  def nonOverlapping[A <: Assignable.Collection](segments1: Iterable[A],
                                                 segments2: Iterable[A])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[A] =
    nonOverlapping(segments1, segments2, segments1.size)

  def nonOverlapping[A <: Assignable.Collection](segments1: Iterable[A],
                                                 segments2: Iterable[A],
                                                 count: Int)(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[A] =
    if (count == 0) {
      Iterable.empty
    } else {
      val resultSegments = ListBuffer.empty[A]
      segments1 foreachBreak {
        segment1 =>
          if (!segments2.exists(segment2 => overlaps(segment1, segment2)))
            resultSegments += segment1
          resultSegments.size == count
      }
      resultSegments
    }

  def overlaps[A <: Assignable.Collection](segments1: Iterable[A],
                                           segments2: Iterable[A])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[A] =
    segments1.filter(segment1 => segments2.exists(segment2 => overlaps(segment1, segment2)))

  def overlaps(segment: Assignable.Collection,
               segments2: Iterable[Assignable.Collection])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    segments2.exists(segment2 => overlaps(segment, segment2))

  def overlapsCount(segment: Assignable.Collection,
                    segments2: Iterable[Assignable.Collection])(implicit keyOrder: KeyOrder[Slice[Byte]]): Int =
    segments2.count(segment2 => overlaps(segment, segment2))

  def containsOne(segments1: Iterable[Segment], segments2: Iterable[Segment]): Boolean =
    if (segments1.isEmpty || segments2.isEmpty)
      false
    else
      segments1.exists(segment1 => segments2.exists(_.path == segment1.path))

  def contains(segment: Segment, segments2: Iterable[Segment]): Boolean =
    segments2.exists(_.path == segment.path)

  def deleteSegments(segments: Iterable[Segment]): Int =
    segments.foldLeftRecover(0, failFast = false) {
      case (deleteCount, segment) =>
        segment.delete()
        deleteCount + 1
    }

  //NOTE: segments should be ordered.
  def tempMinMaxKeyValues(segments: Iterable[Assignable.Collection]): Slice[Memory] =
    segments.foldLeft(Slice.allocate[Memory](segments.size * 2)) {
      case (keyValues, segment) =>
        keyValues add Memory.Put(segment.key, Slice.Null, None, Time.empty)
        segment.maxKey match {
          case MaxKey.Fixed(maxKey) =>
            keyValues add Memory.Put(maxKey, Slice.Null, None, Time.empty)

          case MaxKey.Range(fromKey, maxKey) =>
            keyValues add Memory.Range(fromKey, maxKey, Value.FromValue.Null, Value.Update(maxKey, None, Time.empty))
        }
    }

  @inline def tempMinMaxKeyValuesFrom[I](map: I, head: I => Option[Memory], last: I => Option[Memory]): Slice[Memory] = {
    for {
      minKey <- head(map).map(memory => Memory.Put(memory.key, Slice.Null, None, Time.empty))
      maxKey <- last(map) map {
        case fixed: Memory.Fixed =>
          Memory.Put(fixed.key, Slice.Null, None, Time.empty)

        case Memory.Range(fromKey, toKey, _, _) =>
          Memory.Range(fromKey, toKey, Value.FromValue.Null, Value.Update(Slice.Null, None, Time.empty))
      }
    } yield
      Slice(minKey, maxKey)
  } getOrElse Slice.allocate[Memory](0)

  def tempMinMaxKeyValues(map: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): Slice[Memory] =
    tempMinMaxKeyValuesFrom[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]](map, _.head().toOptionS, _.last().toOptionS)

  def tempMinMaxKeyValues(keyValues: Slice[Memory]): Slice[Memory] =
    tempMinMaxKeyValuesFrom[Slice[Memory]](keyValues, _.headOption, _.lastOption)

  @inline def minMaxKeyFrom[I](input: I, head: I => Option[Memory], last: I => Option[Memory]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    for {
      minKey <- head(input).map(_.key)
      maxKey <- last(input) map {
        case fixed: Memory.Fixed =>
          (fixed.key, true)

        case range: Memory.Range =>
          (range.toKey, false)
      }
    } yield (minKey, maxKey._1, maxKey._2)

  def minMaxKey(keyValues: Either[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], Slice[Memory]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    keyValues match {
      case util.Left(value) =>
        minMaxKey(value)

      case util.Right(value) =>
        minMaxKey(value)
    }

  @inline def minMaxKey(map: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    minMaxKeyFrom[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]](map, _.head().toOptionS, _.last().toOptionS)

  @inline def minMaxKey(map: Slice[Memory]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    minMaxKeyFrom[Slice[Memory]](map, _.headOption, _.lastOption)

  def minMaxKey(segment: Iterable[Assignable.Collection]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    for {
      minKey <- segment.headOption.map(_.key)
      maxKey <- segment.lastOption.map(_.maxKey) map {
        case MaxKey.Fixed(maxKey) =>
          (maxKey, true)

        case MaxKey.Range(_, maxKey) =>
          (maxKey, false)
      }
    } yield {
      (minKey, maxKey._1, maxKey._2)
    }

  def minMaxKey(left: Iterable[Assignable.Collection],
                right: Iterable[Assignable.Collection])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

  def minMaxKey(left: Iterable[Assignable.Collection],
                right: Either[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], Slice[Memory]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    right match {
      case util.Left(right) =>
        Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

      case util.Right(right) =>
        Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))
    }

  def minMaxKey(left: Iterable[Assignable.Collection],
                right: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

  def minMaxKey(left: Iterable[Assignable.Collection],
                right: Slice[Memory])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

  def overlapsWithBusySegments(inputSegments: Iterable[Segment],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    if (busySegments.isEmpty)
      false
    else {
      val assignments =
        Assigner.assignMinMaxOnlyUnsafeNoGaps(
          inputSegments = inputSegments,
          targetSegments = appendixSegments
        )

      Segment.overlaps(
        segments1 = busySegments,
        segments2 = assignments
      ).nonEmpty
    }

  def overlapsWithBusySegments(map: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    if (busySegments.isEmpty)
      false
    else {
      for {
        head <- map.head().toOptionS
        last <- map.last().toOptionS
      } yield {
        val assignments =
          if (keyOrder.equiv(head.key, last.key))
            Assigner.assignUnsafeNoGaps(keyValues = Slice(head), segments = appendixSegments, initialiseIteratorsInOneSeek = false)
          else
            Assigner.assignUnsafeNoGaps(keyValues = Slice(head, last), segments = appendixSegments, initialiseIteratorsInOneSeek = false)

        Segment.overlaps(
          segments1 = busySegments,
          segments2 = assignments.map(_.segment)
        ).nonEmpty
      }
    } getOrElse false

  def getNearestPutDeadline(deadline: Option[Deadline],
                            next: KeyValue): Option[Deadline] =
    next match {
      case readOnly: KeyValue.Put =>
        FiniteDurations.getNearestDeadline(deadline, readOnly.deadline)

      case _: KeyValue.Remove =>
        //        FiniteDurations.getNearestDeadline(deadline, readOnly.deadline)
        deadline

      case _: KeyValue.Update =>
        //        FiniteDurations.getNearestDeadline(deadline, readOnly.deadline)
        deadline

      case _: KeyValue.PendingApply =>
        //        FiniteDurations.getNearestDeadline(deadline, readOnly.deadline)
        deadline

      case _: KeyValue.Function =>
        deadline

      case range: KeyValue.Range =>
        range.fetchFromAndRangeValueUnsafe match {
          case (fromValue: Value.FromValue, _) =>
            getNearestPutDeadline(deadline, fromValue)

          case (Value.FromValue.Null, _) =>
            deadline
        }
    }

  def getNearestPutDeadline(deadline: Option[Deadline],
                            keyValue: Memory): Option[Deadline] =
    keyValue match {
      case writeOnly: Memory.Fixed =>
        FiniteDurations.getNearestDeadline(deadline, writeOnly.deadline)

      case range: Memory.Range =>
        (range.fromValue, range.rangeValue) match {
          case (fromValue: Value.FromValue, rangeValue) =>
            val fromValueDeadline = getNearestPutDeadline(deadline, fromValue)
            getNearestPutDeadline(fromValueDeadline, rangeValue)

          case (Value.FromValue.Null, rangeValue) =>
            getNearestPutDeadline(deadline, rangeValue)
        }
    }

  def getNearestPutDeadline(deadline: Option[Deadline],
                            keyValue: Value.FromValue): Option[Deadline] =
    keyValue match {
      case _: Value.RangeValue =>
        //        getNearestDeadline(deadline, rangeValue)
        deadline

      case put: Value.Put =>
        FiniteDurations.getNearestDeadline(deadline, put.deadline)
    }

  //  def getNearestDeadline(deadline: Option[Deadline],
  //                         rangeValue: Value.RangeValue): Option[Deadline] =
  //    rangeValue match {
  //      case remove: Value.Remove =>
  //        FiniteDurations.getNearestDeadline(deadline, remove.deadline)
  //      case update: Value.Update =>
  //        FiniteDurations.getNearestDeadline(deadline, update.deadline)
  //      case _: Value.Function =>
  //        deadline
  //      case pendingApply: Value.PendingApply =>
  //        FiniteDurations.getNearestDeadline(deadline, pendingApply.deadline)
  //    }

  //  def getNearestDeadline(previous: Option[Deadline],
  //                         applies: Slice[Value.Apply]): Option[Deadline] =
  //    applies.foldLeft(previous) {
  //      case (deadline, apply) =>
  //        getNearestDeadline(
  //          deadline = deadline,
  //          rangeValue = apply
  //        )
  //    }

  def getNearestDeadline(keyValues: Iterable[KeyValue]): Option[Deadline] =
    keyValues.foldLeftRecover(Option.empty[Deadline])(getNearestPutDeadline)

  def getNearestDeadlineSegment(previous: Segment,
                                next: Segment): SegmentOption =
    (previous.nearestPutDeadline, next.nearestPutDeadline) match {
      case (None, None) =>
        Segment.Null

      case (Some(_), None) =>
        previous

      case (None, Some(_)) =>
        next

      case (Some(previousDeadline), Some(nextDeadline)) =>
        if (previousDeadline < nextDeadline)
          previous
        else
          next
    }

  def getNearestDeadlineSegment(segments: Iterable[Segment]): SegmentOption =
    segments.foldLeft(Segment.Null: SegmentOption) {
      case (previous, next) =>
        previous mapS {
          previous =>
            getNearestDeadlineSegment(previous, next)
        } getOrElse {
          if (next.nearestPutDeadline.isDefined)
            next
          else
            Segment.Null
        }
    }

  def toMemoryIterator(fullIterator: Iterator[KeyValue],
                       removeDeletes: Boolean): Iterator[Memory] =
    new Iterator[Memory] {

      var nextOne: Memory = _

      //FIXME - hasNext jumps to next item even if next() was not invoked.
      @tailrec
      final override def hasNext: Boolean =
        if (fullIterator.hasNext) {
          val nextKeyValue = fullIterator.next()
          val nextKeyValueOrNull =
            if (removeDeletes)
              KeyValueGrouper.toLastLevelOrNull(nextKeyValue)
            else
              nextKeyValue.toMemory()

          if (nextKeyValueOrNull == null) {
            hasNext
          } else {
            nextOne = nextKeyValueOrNull
            true
          }
        } else {
          false
        }

      override def next(): Memory =
        nextOne
    }
}

private[core] trait Segment extends FileSweeperItem with SegmentOption with Assignable.Collection with Distributable { self =>

  final def key: Slice[Byte] =
    minKey

  def minKey: Slice[Byte]

  def maxKey: MaxKey[Slice[Byte]]

  def segmentSize: Int

  def nearestPutDeadline: Option[Deadline]

  def minMaxFunctionId: Option[MinMax[Slice[Byte]]]

  def formatId: Byte

  def createdInLevel: Int

  def path: Path

  def isMMAP: Boolean

  def segmentNumber: Long =
    Effect.numberFileId(path)._1

  def getFromCache(key: Slice[Byte]): KeyValueOption

  def mightContainKey(key: Slice[Byte], threadState: ThreadReadState): Boolean

  def mightContainFunction(key: Slice[Byte]): Boolean

  def get(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption

  def lower(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption

  def higher(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption

  def iterator(initialiseIteratorsInOneSeek: Boolean): Iterator[KeyValue]

  def delete(): Unit

  def delete(delay: FiniteDuration): Unit

  def close(): Unit

  def keyValueCount: Int

  def clearCachedKeyValues(): Unit

  def clearAllCaches(): Unit

  def isInKeyValueCache(key: Slice[Byte]): Boolean

  def isKeyValueCacheEmpty: Boolean

  def areAllCachesEmpty: Boolean

  def cachedKeyValueSize: Int

  def hasRange: Boolean =
    rangeCount > 0

  def updateCount: Int

  def rangeCount: Int

  def putCount: Int

  def putDeadlineCount: Int

  def hasExpired(): Boolean =
    nearestPutDeadline.exists(_.isOverdue())

  def hasUpdateOrRange: Boolean =
    updateCount > 0 || rangeCount > 0

  def hasUpdateOrRangeOrExpired(): Boolean =
    hasUpdateOrRange || hasExpired()

  def isFooterDefined: Boolean

  def isOpen: Boolean

  def isCached: Boolean

  def memory: Boolean

  def persistent: Boolean

  def existsOnDisk(): Boolean

  def existsOnDiskOrMemory(): Boolean

  def hasBloomFilter(): Boolean

  override def isNoneS: Boolean =
    false

  override def getS: Segment =
    this

  override def equals(other: Any): Boolean =
    other match {
      case other: Segment =>
        this.path == other.path

      case _ =>
        false
    }

  override def hashCode(): Int =
    path.hashCode()
}
