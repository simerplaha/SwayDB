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

package swaydb.core.data

import swaydb.compression.CompressionInternal
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.group.compression.data.GroupHeader
import swaydb.core.group.compression.{GroupCompressor, GroupDecompressor, GroupKeyCompressor}
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.{RangeValueSerializer, ValueSerializer}
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.format.a.entry.reader.value._
import swaydb.core.segment.format.a.entry.writer._
import swaydb.core.segment.{Segment, SegmentCache, SegmentCacheInitializer}
import swaydb.core.util.Bytes
import swaydb.core.util.CollectionUtil._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.{IO, MaxKey}

import scala.collection.SortedSet
import scala.concurrent.duration.{Deadline, FiniteDuration}

private[core] sealed trait KeyValue {
  def key: Slice[Byte]

  def keyLength =
    key.size
}

private[core] object KeyValue {

  /**
    * Read-only instances are only created for Key-values read from disk for Persistent Segments
    * and are stored in-memory after merge for Memory Segments.
    */
  sealed trait ReadOnly extends KeyValue {
    def indexEntryDeadline: Option[Deadline]
  }

  /**
    * Key-values that can be added to [[KeyValueLimiter]].
    *
    * These key-values can remain in memory depending on the cacheSize and are dropped or uncompressed on overflow.
    *
    * Only [[KeyValue.ReadOnly.Group]] && [[Persistent.SegmentResponse]] key-values are [[CacheAble]].
    *
    * Only [[Memory.Group]] key-values are uncompressed and every other key-value is dropped.
    */
  sealed trait CacheAble extends ReadOnly {
    def valueLength: Int
  }

  object ReadOnly {
    /**
      * An API response type expected from a [[swaydb.core.map.Map]] or [[swaydb.core.segment.Segment]].
      *
      * Key-value types like [[Group]] are processed within [[swaydb.core.map.Map]] or [[swaydb.core.segment.Segment]].
      */
    sealed trait SegmentResponse extends KeyValue with ReadOnly

    sealed trait Fixed extends SegmentResponse {
      def toFromValue(): IO[Value.FromValue]

      def toRangeValue(): IO[Value.RangeValue]

      def time: Time
    }

    sealed trait Put extends KeyValue.ReadOnly.Fixed {
      def valueLength: Int
      def deadline: Option[Deadline]
      def hasTimeLeft(): Boolean
      def isOverdue(): Boolean = !hasTimeLeft()
      def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean
      def getOrFetchValue: IO[Option[Slice[Byte]]]
      def time: Time
      def toFromValue(): IO[Value.Put]
      def copyWithDeadlineAndTime(deadline: Option[Deadline], time: Time): KeyValue.ReadOnly.Put
      def copyWithTime(time: Time): KeyValue.ReadOnly.Put
    }

    sealed trait Remove extends KeyValue.ReadOnly.Fixed {
      def deadline: Option[Deadline]
      def hasTimeLeft(): Boolean
      def isOverdue(): Boolean = !hasTimeLeft()
      def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean
      def time: Time
      def toFromValue(): IO[Value.Remove]
      def toRemoveValue(): Value.Remove
      def copyWithTime(time: Time): KeyValue.ReadOnly.Remove
    }

    sealed trait Update extends KeyValue.ReadOnly.Fixed {
      def deadline: Option[Deadline]
      def hasTimeLeft(): Boolean
      def isOverdue(): Boolean = !hasTimeLeft()
      def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean
      def time: Time
      def getOrFetchValue: IO[Option[Slice[Byte]]]
      def toFromValue(): IO[Value.Update]
      def toPut(): KeyValue.ReadOnly.Put
      def toPut(deadline: Option[Deadline]): KeyValue.ReadOnly.Put
      def copyWithDeadlineAndTime(deadline: Option[Deadline], time: Time): KeyValue.ReadOnly.Update
      def copyWithDeadline(deadline: Option[Deadline]): KeyValue.ReadOnly.Update
      def copyWithTime(time: Time): KeyValue.ReadOnly.Update
    }

    sealed trait Function extends KeyValue.ReadOnly.Fixed {
      def time: Time
      def getOrFetchFunction: IO[Slice[Byte]]
      def toFromValue(): IO[Value.Function]
      def copyWithTime(time: Time): Function
    }

    sealed trait PendingApply extends KeyValue.ReadOnly.Fixed {
      def getOrFetchApplies: IO[Slice[Value.Apply]]
      def toFromValue(): IO[Value.PendingApply]
      def time: Time
      def deadline: Option[Deadline]
    }

    object Range {
      implicit class RangeImplicit(range: KeyValue.ReadOnly.Range) {
        def contains(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
          import keyOrder._
          key >= range.fromKey && key < range.toKey
        }

        def containsLower(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
          import keyOrder._
          key > range.fromKey && key <= range.toKey
        }
      }
    }

    sealed trait Range extends KeyValue.ReadOnly with SegmentResponse {
      def fromKey: Slice[Byte]
      def toKey: Slice[Byte]
      def fetchFromValue: IO[Option[Value.FromValue]]
      def fetchRangeValue: IO[Value.RangeValue]
      def fetchFromAndRangeValue: IO[(Option[Value.FromValue], Value.RangeValue)]
      def fetchFromOrElseRangeValue: IO[Value.FromValue] =
        fetchFromAndRangeValue map {
          case (fromValue, rangeValue) =>
            fromValue getOrElse rangeValue
        }
    }

    object Group {
      implicit class GroupImplicit(group: KeyValue.ReadOnly.Group) {
        def contains(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
          import keyOrder._
          key >= group.minKey && ((group.maxKey.inclusive && key <= group.maxKey.maxKey) || (!group.maxKey.inclusive && key < group.maxKey.maxKey))
        }

        def containsHigher(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
          import keyOrder._
          key >= group.minKey && key < group.maxKey.maxKey
        }

        def containsLower(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
          import keyOrder._
          key > group.minKey && key <= group.maxKey.maxKey
        }
      }
    }

    sealed trait Group extends KeyValue.ReadOnly with CacheAble {
      def minKey: Slice[Byte]
      def maxKey: MaxKey[Slice[Byte]]
      def header(): IO[GroupHeader]
      def segmentCache(implicit keyOrder: KeyOrder[Slice[Byte]],
                       keyValueLimiter: KeyValueLimiter): SegmentCache
      def deadline: Option[Deadline]
    }
  }

  /**
    * Write-only instances are only created after a successful merge of key-values and are used to write to Persistent
    * and Memory Segments.
    */
  sealed trait WriteOnly extends KeyValue {
    val isRemoveRangeMayBe: Boolean
    val isRange: Boolean
    val isGroup: Boolean
    val previous: Option[KeyValue.WriteOnly]
    def enableRangeFilterAndIndex: Boolean
    def resetPrefixCompressionEvery: Int
    def minimumNumberOfKeysForHashIndex: Int
    def hashIndexCompensation: Int => Int
    def value: Option[Slice[Byte]]
    def fullKey: Slice[Byte]
    def stats: Stats
    def deadline: Option[Deadline]
    def indexEntryBytes: Slice[Byte]
    def valueEntryBytes: Option[Slice[Byte]]
    //a flag that returns true if valueBytes are created for this or any of it's previous key-values indicating value slice is required.
    def hasValueEntryBytes: Boolean
    //start value offset is carried current value offset position.
    def currentStartValueOffsetPosition: Int
    def currentEndValueOffsetPosition: Int
    def nextStartValueOffsetPosition: Int =
      if (!hasValueEntryBytes && currentEndValueOffsetPosition == 0)
        0
      else
        currentEndValueOffsetPosition + 1

    def enablePrefixCompression: Boolean =
      resetPrefixCompressionEvery > 0 &&
        previous.exists {
          previous =>
            previous.stats.position + 1 % resetPrefixCompressionEvery != 0
        }

    def updateStats(falsePositiveRate: Double,
                    previous: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly

    private def thisInScope = this

    def reverseIterator: Iterator[WriteOnly] =
      new Iterator[WriteOnly] {
        var currentPrevious: Option[KeyValue.WriteOnly] =
          Some(thisInScope)

        override def hasNext: Boolean =
          currentPrevious.isDefined

        override def next(): KeyValue.WriteOnly = {
          val next = currentPrevious.get
          currentPrevious = next.previous
          next
        }
      }
  }

  object WriteOnly {

    implicit class WriteOnlyImplicits(keyValues: Iterable[KeyValue.WriteOnly]) {
      def lastGroup(): Option[Transient.Group] =
        keyValues.iterator.foldLeftWhile(Option.empty[Transient.Group], _.isGroup) {
          case (_, group: Transient.Group) =>
            Some(group)
          case (previousGroup, _) =>
            previousGroup
        }

      def maxKey() =
        keyValues.last match {
          case range: Range =>
            MaxKey.Range(range.fromKey, range.toKey)
          case group: Group =>
            group.maxKey
          case fixed: Transient =>
            MaxKey.Fixed(fixed.key)
        }

      def minKey: Slice[Byte] =
        keyValues.head.key
    }

    sealed trait Fixed extends KeyValue.WriteOnly {

      def hasTimeLeft(): Boolean
      def isOverdue(): Boolean = !hasTimeLeft()
      def time: Time
    }

    sealed trait Range extends KeyValue.WriteOnly {
      def fromKey: Slice[Byte]
      def toKey: Slice[Byte]
      def fromValue: Option[Value.FromValue]
      def rangeValue: Value.RangeValue
      def fetchFromValue: IO[Option[Value.FromValue]]
      def fetchRangeValue: IO[Value.RangeValue]
      def fetchFromAndRangeValue: IO[(Option[Value.FromValue], Value.RangeValue)]
    }

    sealed trait Group extends KeyValue.WriteOnly {
      def minKey: Slice[Byte]
      def maxKey: MaxKey[Slice[Byte]]
      def fullKey: Slice[Byte]
      def keyValues: Slice[KeyValue.WriteOnly]
      def compressedKeyValues: Slice[Byte]
    }
  }

  type KeyValueTuple = (Slice[Byte], Option[Slice[Byte]])
}

private[swaydb] sealed trait Memory extends KeyValue.ReadOnly {

  def key: Slice[Byte]
}

private[swaydb] object Memory {
  sealed trait SegmentResponse extends Memory with KeyValue.ReadOnly.SegmentResponse

  sealed trait Fixed extends Memory.SegmentResponse with KeyValue.ReadOnly.Fixed

  case class Put(key: Slice[Byte],
                 value: Option[Slice[Byte]],
                 deadline: Option[Deadline],
                 time: Time) extends Memory.Fixed with KeyValue.ReadOnly.Put {

    override def indexEntryDeadline: Option[Deadline] = deadline

    override def valueLength: Int =
      value.map(_.size).getOrElse(0)

    def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean =
      deadline.forall(deadline => (deadline - minus).hasTimeLeft())

    override def getOrFetchValue: IO[Option[Slice[Byte]]] =
      IO.Success(value)

    override def toFromValue(): IO[Value.Put] =
      IO.Success(Value.Put(value, deadline, time))

    override def copyWithDeadlineAndTime(deadline: Option[Deadline],
                                         time: Time): Put =
      copy(deadline = deadline, time = time)

    override def copyWithTime(time: Time): Put =
      copy(time = time)

    //ahh not very type-safe.
    override def toRangeValue(): IO[Value.RangeValue] =
      IO.Failure(new Exception("Put cannot be converted to RangeValue"))
  }

  case class Update(key: Slice[Byte],
                    value: Option[Slice[Byte]],
                    deadline: Option[Deadline],
                    time: Time) extends KeyValue.ReadOnly.Update with Memory.Fixed {

    override def indexEntryDeadline: Option[Deadline] = deadline

    def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean =
      deadline.forall(deadline => (deadline - minus).hasTimeLeft())

    override def getOrFetchValue: IO[Option[Slice[Byte]]] =
      IO.Success(value)

    override def toFromValue(): IO[Value.Update] =
      IO.Success(Value.Update(value, deadline, time))

    override def copyWithDeadlineAndTime(deadline: Option[Deadline],
                                         time: Time): Update =
      copy(deadline = deadline, time = time)

    override def copyWithTime(time: Time): Update =
      copy(time = time)

    override def copyWithDeadline(deadline: Option[Deadline]): Update =
      copy(deadline = deadline)

    override def toPut(): Memory.Put =
      Memory.Put(
        key = key,
        value = value,
        deadline = deadline,
        time = time
      )

    override def toPut(deadline: Option[Deadline]): Memory.Put =
      Memory.Put(
        key = key,
        value = value,
        deadline = deadline,
        time = time
      )

    override def toRangeValue(): IO[Value.Update] =
      toFromValue()
  }

  case class Function(key: Slice[Byte],
                      function: Slice[Byte],
                      time: Time) extends KeyValue.ReadOnly.Function with Memory.Fixed {

    override def indexEntryDeadline: Option[Deadline] = None

    override def getOrFetchFunction: IO[Slice[Byte]] =
      IO.Success(function)

    override def toFromValue(): IO[Value.Function] =
      IO.Success(Value.Function(function, time))

    override def copyWithTime(time: Time): Function =
      copy(time = time)

    override def toRangeValue(): IO[Value.Function] =
      toFromValue()
  }

  case class PendingApply(key: Slice[Byte],
                          applies: Slice[Value.Apply]) extends KeyValue.ReadOnly.PendingApply with Memory.Fixed {

    override val deadline =
      Segment.getNearestDeadline(None, applies)

    override def indexEntryDeadline: Option[Deadline] = deadline

    def time = Time.fromApplies(applies)

    override def getOrFetchApplies: IO[Slice[Value.Apply]] =
      IO.Success(applies)

    override def toFromValue(): IO[Value.PendingApply] =
      IO.Success(Value.PendingApply(applies))

    override def toRangeValue(): IO[Value.PendingApply] =
      toFromValue()
  }

  case class Remove(key: Slice[Byte],
                    deadline: Option[Deadline],
                    time: Time) extends Memory.Fixed with KeyValue.ReadOnly.Remove {

    override def indexEntryDeadline: Option[Deadline] = deadline

    def hasTimeLeft(): Boolean =
      deadline.exists(_.hasTimeLeft())

    def hasTimeLeftAtLeast(atLeast: FiniteDuration): Boolean =
      deadline.exists(deadline => (deadline - atLeast).hasTimeLeft())

    def toRemoveValue(): Value.Remove =
      Value.Remove(deadline, time)

    override def copyWithTime(time: Time): ReadOnly.Remove =
      copy(time = time)

    override def toFromValue(): IO[Value.Remove] =
      IO.Success(toRemoveValue())

    override def toRangeValue(): IO[Value.Remove] =
      toFromValue()
  }

  object Range {
    def apply(fromKey: Slice[Byte],
              toKey: Slice[Byte],
              fromValue: Value.FromValue,
              rangeValue: Value.RangeValue): Range =
      new Range(fromKey, toKey, Some(fromValue), rangeValue)
  }

  case class Range(fromKey: Slice[Byte],
                   toKey: Slice[Byte],
                   fromValue: Option[Value.FromValue],
                   rangeValue: Value.RangeValue) extends Memory.SegmentResponse with KeyValue.ReadOnly.Range {

    override def key: Slice[Byte] = fromKey

    override def indexEntryDeadline: Option[Deadline] = None

    override def fetchFromValue: IO[Option[Value.FromValue]] =
      IO.Success(fromValue)

    override def fetchRangeValue: IO[Value.RangeValue] =
      IO.Success(rangeValue)

    override def fetchFromAndRangeValue: IO[(Option[Value.FromValue], Value.RangeValue)] =
      IO.Success(fromValue, rangeValue)
  }

  object Group {
    def apply(minKey: Slice[Byte],
              maxKey: MaxKey[Slice[Byte]],
              compressedKeyValues: Slice[Byte],
              groupStartOffset: Int,
              deadline: Option[Deadline]): Memory.Group =
      new Group(
        minKey = minKey,
        maxKey = maxKey,
        deadline = deadline,
        valueLength = compressedKeyValues.size,
        groupDecompressor = GroupDecompressor(Reader(compressedKeyValues), groupStartOffset)
      )
  }

  case class Group(minKey: Slice[Byte],
                   maxKey: MaxKey[Slice[Byte]],
                   deadline: Option[Deadline],
                   groupDecompressor: GroupDecompressor,
                   valueLength: Int) extends Memory with KeyValue.ReadOnly.Group {

    lazy val segmentCacheInitializer: SegmentCacheInitializer =
      new SegmentCacheInitializer(
        id = "Persistent.Group",
        minKey = minKey,
        maxKey = maxKey,
        unsliceKey = false,
        getFooter = groupDecompressor.footer _,
        getHashIndexHeader = groupDecompressor.hashIndexHeader _,
        createReader = groupDecompressor.reader _
      )

    override def indexEntryDeadline: Option[Deadline] = deadline

    override def key: Slice[Byte] = minKey

    def isValueDefined: Boolean =
      groupDecompressor.isIndexDecompressed()

    def segmentCache(implicit keyOrder: KeyOrder[Slice[Byte]],
                     keyValueLimiter: KeyValueLimiter): SegmentCache =
      segmentCacheInitializer.segmentCache

    def header() =
      groupDecompressor.header()

    def isHeaderDecompressed: Boolean =
      groupDecompressor.isHeaderDecompressed()

    def isIndexDecompressed: Boolean =
      groupDecompressor.isIndexDecompressed()

    def isValueDecompressed: Boolean =
      groupDecompressor.isValueDecompressed()

    def uncompress(): Memory.Group =
      copy(groupDecompressor = groupDecompressor.uncompress())
  }

}

private[core] sealed trait Transient extends KeyValue.WriteOnly

private[core] object Transient {

  private[core] sealed trait SegmentResponse extends Transient

  implicit class TransientImplicits(transient: Transient.SegmentResponse)(implicit keyOrder: KeyOrder[Slice[Byte]]) {

    def toMemoryResponse: Memory.SegmentResponse =
      transient match {
        case put: Transient.Put =>
          Memory.Put(put.key, put.value, put.deadline, put.time)

        case remove: Transient.Remove =>
          Memory.Remove(remove.key, remove.deadline, remove.time)

        case function: Transient.Function =>
          Memory.Function(function.key, function.function, function.time)

        case apply: Transient.PendingApply =>
          Memory.PendingApply(apply.key, apply.applies)

        case update: Transient.Update =>
          Memory.Update(
            key = update.key,
            value = update.value,
            deadline = update.deadline,
            time = update.time
          )

        case range: Transient.Range =>
          Memory.Range(
            fromKey = range.fromKey,
            toKey = range.toKey,
            fromValue = range.fromValue,
            rangeValue = range.rangeValue
          )
      }
  }

  case class Remove(key: Slice[Byte],
                    deadline: Option[Deadline],
                    time: Time,
                    previous: Option[KeyValue.WriteOnly],
                    falsePositiveRate: Double,
                    resetPrefixCompressionEvery: Int,
                    minimumNumberOfKeysForHashIndex: Int,
                    hashIndexCompensation: Int => Int,
                    enableRangeFilterAndIndex: Boolean) extends Transient.SegmentResponse with KeyValue.WriteOnly.Fixed {
    override val isRange: Boolean = false
    override val isGroup: Boolean = false
    override val isRemoveRangeMayBe = false
    override val value: Option[Slice[Byte]] = None
    override val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = false,
        enablePrefixCompression = enablePrefixCompression
      ).unapply

    override def fullKey = key

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)
    override val stats =
      Stats(
        indexEntry = indexEntryBytes,
        value = None,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = false,
        position = previous.map(_.stats.position + 1) getOrElse 1,
        hashIndexItemsCount = previous.map(_.stats.hashIndexItemsCount + 1) getOrElse 1,
        numberOfRanges = 0,
        bloomFiltersItemCount = 1,
        usePreviousHashIndexOffset = enablePrefixCompression,
        minimumNumberOfKeysForHashIndex = minimumNumberOfKeysForHashIndex,
        hashIndexCompensation = hashIndexCompensation,
        enableRangeFilterAndIndex = enableRangeFilterAndIndex,
        rangeCommonPrefixesCount = previous.map(_.stats.rangeCommonPrefixesCount).getOrElse(Stats.emptyRangeCommonPrefixesCount),
        previous = previous,
        deadline = deadline
      )

    override def updateStats(falsePositiveRate: Double,
                             keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(previous = keyValue, falsePositiveRate = falsePositiveRate)

    override def hasTimeLeft(): Boolean =
      deadline.exists(_.hasTimeLeft())
  }

  case class Put(key: Slice[Byte],
                 value: Option[Slice[Byte]],
                 deadline: Option[Deadline],
                 time: Time,
                 previous: Option[KeyValue.WriteOnly],
                 falsePositiveRate: Double,
                 compressDuplicateValues: Boolean,
                 resetPrefixCompressionEvery: Int,
                 minimumNumberOfKeysForHashIndex: Int,
                 hashIndexCompensation: Int => Int,
                 enableRangeFilterAndIndex: Boolean) extends Transient.SegmentResponse with KeyValue.WriteOnly.Fixed {

    override val isRemoveRangeMayBe = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = compressDuplicateValues,
        enablePrefixCompression = enablePrefixCompression
      ).unapply

    override val hasValueEntryBytes: Boolean =
      previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = true,
        position = previous.map(_.stats.position + 1) getOrElse 1,
        hashIndexItemsCount = previous.map(_.stats.hashIndexItemsCount + 1) getOrElse 1,
        numberOfRanges = 0,
        bloomFiltersItemCount = 1,
        usePreviousHashIndexOffset = enablePrefixCompression,
        minimumNumberOfKeysForHashIndex = minimumNumberOfKeysForHashIndex,
        hashIndexCompensation = hashIndexCompensation,
        enableRangeFilterAndIndex = enableRangeFilterAndIndex,
        rangeCommonPrefixesCount = previous.map(_.stats.rangeCommonPrefixesCount).getOrElse(Stats.emptyRangeCommonPrefixesCount),
        previous = previous,
        deadline = deadline
      )

    override def fullKey = key

    override def updateStats(falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(falsePositiveRate = falsePositiveRate, previous = previous)

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())
  }

  case class Update(key: Slice[Byte],
                    value: Option[Slice[Byte]],
                    deadline: Option[Deadline],
                    time: Time,
                    previous: Option[KeyValue.WriteOnly],
                    falsePositiveRate: Double,
                    compressDuplicateValues: Boolean,
                    resetPrefixCompressionEvery: Int,
                    minimumNumberOfKeysForHashIndex: Int,
                    hashIndexCompensation: Int => Int,
                    enableRangeFilterAndIndex: Boolean) extends Transient.SegmentResponse with KeyValue.WriteOnly.Fixed {
    override val isRemoveRangeMayBe = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false
    override def fullKey = key

    override def updateStats(falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Transient.Update =
      this.copy(falsePositiveRate = falsePositiveRate, previous = previous)

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = compressDuplicateValues,
        enablePrefixCompression = enablePrefixCompression
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = false,
        position = previous.map(_.stats.position + 1) getOrElse 1,
        hashIndexItemsCount = previous.map(_.stats.hashIndexItemsCount + 1) getOrElse 1,
        numberOfRanges = 0,
        bloomFiltersItemCount = 1,
        usePreviousHashIndexOffset = enablePrefixCompression,
        minimumNumberOfKeysForHashIndex = minimumNumberOfKeysForHashIndex,
        hashIndexCompensation = hashIndexCompensation,
        enableRangeFilterAndIndex = enableRangeFilterAndIndex,
        rangeCommonPrefixesCount = previous.map(_.stats.rangeCommonPrefixesCount).getOrElse(Stats.emptyRangeCommonPrefixesCount),
        previous = previous,
        deadline = deadline
      )
  }

  case class Function(key: Slice[Byte],
                      function: Slice[Byte],
                      deadline: Option[Deadline],
                      time: Time,
                      previous: Option[KeyValue.WriteOnly],
                      falsePositiveRate: Double,
                      compressDuplicateValues: Boolean,
                      resetPrefixCompressionEvery: Int,
                      minimumNumberOfKeysForHashIndex: Int,
                      hashIndexCompensation: Int => Int,
                      enableRangeFilterAndIndex: Boolean) extends Transient.SegmentResponse with KeyValue.WriteOnly.Fixed {
    override val isRemoveRangeMayBe = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false

    override def fullKey = key

    override def value = Some(function)

    override def updateStats(falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Transient.Function =
      this.copy(falsePositiveRate = falsePositiveRate, previous = previous)

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = compressDuplicateValues,
        enablePrefixCompression = enablePrefixCompression
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = false,
        position = previous.map(_.stats.position + 1) getOrElse 1,
        hashIndexItemsCount = previous.map(_.stats.hashIndexItemsCount + 1) getOrElse 1,
        numberOfRanges = 0,
        bloomFiltersItemCount = 1,
        usePreviousHashIndexOffset = enablePrefixCompression,
        minimumNumberOfKeysForHashIndex = minimumNumberOfKeysForHashIndex,
        hashIndexCompensation = hashIndexCompensation,
        enableRangeFilterAndIndex = enableRangeFilterAndIndex,
        rangeCommonPrefixesCount = previous.map(_.stats.rangeCommonPrefixesCount).getOrElse(Stats.emptyRangeCommonPrefixesCount),
        previous = previous,
        deadline = deadline
      )
  }

  case class PendingApply(key: Slice[Byte],
                          applies: Slice[Value.Apply],
                          previous: Option[KeyValue.WriteOnly],
                          falsePositiveRate: Double,
                          compressDuplicateValues: Boolean,
                          resetPrefixCompressionEvery: Int,
                          minimumNumberOfKeysForHashIndex: Int,
                          hashIndexCompensation: Int => Int,
                          enableRangeFilterAndIndex: Boolean) extends Transient.SegmentResponse with KeyValue.WriteOnly.Fixed {
    override val isRemoveRangeMayBe = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false
    override val deadline: Option[Deadline] =
      Segment.getNearestDeadline(None, applies)

    override def value: Option[Slice[Byte]] = {
      val bytesRequired = ValueSerializer.bytesRequired(applies)
      val bytes = Slice.create[Byte](bytesRequired)
      ValueSerializer.write(applies)(bytes)
      Some(bytes)
    }

    def time = Time.fromApplies(applies)

    override def fullKey = key

    override def updateStats(falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Transient.PendingApply =
      this.copy(falsePositiveRate = falsePositiveRate, previous = previous)

    override def hasTimeLeft(): Boolean =
      true

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = compressDuplicateValues,
        enablePrefixCompression = enablePrefixCompression
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = false,
        position = previous.map(_.stats.position + 1) getOrElse 1,
        hashIndexItemsCount = previous.map(_.stats.hashIndexItemsCount + 1) getOrElse 1,
        numberOfRanges = 0,
        bloomFiltersItemCount = 1,
        usePreviousHashIndexOffset = enablePrefixCompression,
        minimumNumberOfKeysForHashIndex = minimumNumberOfKeysForHashIndex,
        hashIndexCompensation = hashIndexCompensation,
        enableRangeFilterAndIndex = enableRangeFilterAndIndex,
        rangeCommonPrefixesCount = previous.map(_.stats.rangeCommonPrefixesCount).getOrElse(Stats.emptyRangeCommonPrefixesCount),
        previous = previous,
        deadline = deadline
      )
  }

  object Range {

    def apply[R <: Value.RangeValue](fromKey: Slice[Byte],
                                     toKey: Slice[Byte],
                                     rangeValue: R,
                                     falsePositiveRate: Double,
                                     resetPrefixCompressionEvery: Int,
                                     minimumNumberOfKeyForHashIndex: Int,
                                     hashIndexCompensation: Int => Int,
                                     enableRangeFilterAndIndex: Boolean,
                                     previous: Option[KeyValue.WriteOnly])(implicit rangeValueSerializer: RangeValueSerializer[Unit, R]): Range = {
      val bytesRequired = rangeValueSerializer.bytesRequired((), rangeValue)
      val value = if (bytesRequired == 0) None else Some(Slice.create[Byte](bytesRequired))
      value.foreach(rangeValueSerializer.write((), rangeValue, _))
      val fullKey = Bytes.compressJoin(fromKey, toKey)
      new Range(
        fromKey = fromKey,
        toKey = toKey,
        fullKey = fullKey,
        fromValue = None,
        rangeValue = rangeValue,
        value = value,
        previous = previous,
        falsePositiveRate = falsePositiveRate,
        resetPrefixCompressionEvery = resetPrefixCompressionEvery,
        minimumNumberOfKeysForHashIndex = minimumNumberOfKeyForHashIndex,
        enableRangeFilterAndIndex = enableRangeFilterAndIndex,
        hashIndexCompensation = hashIndexCompensation
      )
    }

    def apply[F <: Value.FromValue, R <: Value.RangeValue](fromKey: Slice[Byte],
                                                           toKey: Slice[Byte],
                                                           fromValue: Option[F],
                                                           rangeValue: R,
                                                           falsePositiveRate: Double,
                                                           resetPrefixCompressionEvery: Int,
                                                           minimumNumberOfKeyForHashIndex: Int,
                                                           hashIndexCompensation: Int => Int,
                                                           enableRangeFilterAndIndex: Boolean,
                                                           previous: Option[KeyValue.WriteOnly])(implicit rangeValueSerializer: RangeValueSerializer[Option[F], R]): Range = {
      val bytesRequired = rangeValueSerializer.bytesRequired(fromValue, rangeValue)
      val value = if (bytesRequired == 0) None else Some(Slice.create[Byte](bytesRequired))
      value.foreach(rangeValueSerializer.write(fromValue, rangeValue, _))
      val fullKey: Slice[Byte] = Bytes.compressJoin(fromKey, toKey)

      new Range(
        fromKey = fromKey,
        toKey = toKey,
        fullKey = fullKey,
        fromValue = fromValue,
        rangeValue = rangeValue,
        value = value,
        previous = previous,
        falsePositiveRate = falsePositiveRate,
        resetPrefixCompressionEvery = resetPrefixCompressionEvery,
        minimumNumberOfKeysForHashIndex = minimumNumberOfKeyForHashIndex,
        enableRangeFilterAndIndex = enableRangeFilterAndIndex,
        hashIndexCompensation = hashIndexCompensation
      )
    }
  }

  case class Range(fromKey: Slice[Byte],
                   toKey: Slice[Byte],
                   fullKey: Slice[Byte],
                   fromValue: Option[Value.FromValue],
                   rangeValue: Value.RangeValue,
                   value: Option[Slice[Byte]],
                   previous: Option[KeyValue.WriteOnly],
                   falsePositiveRate: Double,
                   resetPrefixCompressionEvery: Int,
                   minimumNumberOfKeysForHashIndex: Int,
                   hashIndexCompensation: Int => Int,
                   enableRangeFilterAndIndex: Boolean) extends Transient.SegmentResponse with KeyValue.WriteOnly.Range {

    def key = fromKey

    override val isRemoveRangeMayBe = rangeValue.hasRemoveMayBe
    override val isGroup: Boolean = false
    override val isRange: Boolean = true
    override val deadline: Option[Deadline] = None
    override def updateStats(falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Transient.Range =
      this.copy(falsePositiveRate = falsePositiveRate, previous = previous)

    override def fetchFromValue: IO[Option[Value.FromValue]] =
      IO.Success(fromValue)

    override def fetchRangeValue: IO[Value.RangeValue] =
      IO.Success(rangeValue)

    override def fetchFromAndRangeValue: IO[(Option[Value.FromValue], Value.RangeValue)] =
      IO.Success(fromValue, rangeValue)

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      KeyValueWriter.write(
        current = this,
        currentTime = Time.empty,
        //It's highly likely that two sequential key-values within the same range have the different value after the range split occurs so this is always set to true.
        compressDuplicateValues = true,
        enablePrefixCompression = enablePrefixCompression
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val commonBytesCount = Bytes.commonPrefixBytesCount(fromKey, toKey)

    val rangeCommonPrefixesCount: SortedSet[Int] =
      previous map {
        previous =>
          if (previous.stats.rangeCommonPrefixesCount.contains(commonBytesCount))
            previous.stats.rangeCommonPrefixesCount
          else
            previous.stats.rangeCommonPrefixesCount + commonBytesCount
      } getOrElse Stats.createRangeCommonPrefixesCount(commonBytesCount)

    val stats =
      Stats(
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = fromValue.exists(_.isInstanceOf[Value.Put]),
        position = previous.map(_.stats.position + 1) getOrElse 1,
        hashIndexItemsCount = previous.map(_.stats.hashIndexItemsCount + 2) getOrElse 2,
        numberOfRanges = 1,
        bloomFiltersItemCount = 2,
        usePreviousHashIndexOffset = enablePrefixCompression,
        enableRangeFilterAndIndex = enableRangeFilterAndIndex,
        minimumNumberOfKeysForHashIndex = minimumNumberOfKeysForHashIndex, //ranges cost 2. One for fromKey and second for rangeFilter's common prefix bytes.
        hashIndexCompensation = hashIndexCompensation,
        rangeCommonPrefixesCount = rangeCommonPrefixesCount,
        previous = previous,
        deadline = None
      )
  }

  object Group {

    def apply(keyValues: Slice[KeyValue.WriteOnly],
              indexCompression: CompressionInternal,
              valueCompression: CompressionInternal,
              falsePositiveRate: Double,
              resetPrefixCompressionEvery: Int,
              minimumNumberOfKeyForHashIndex: Int,
              hashIndexCompensation: Int => Int,
              previous: Option[KeyValue.WriteOnly],
              maxProbe: Int,
              enableRangeFilterAndIndex: Boolean): IO[Option[Transient.Group]] =
      GroupCompressor.compress(
        keyValues = keyValues,
        indexCompressions = Seq(indexCompression),
        valueCompressions = Seq(valueCompression),
        falsePositiveRate = falsePositiveRate,
        hashIndexCompensation = hashIndexCompensation,
        resetPrefixCompressionEvery = resetPrefixCompressionEvery,
        minimumNumberOfKeyForHashIndex = minimumNumberOfKeyForHashIndex,
        previous = previous,
        maxProbe = maxProbe,
        enableRangeFilterAndIndex = enableRangeFilterAndIndex
      )

    def apply(keyValues: Slice[KeyValue.WriteOnly],
              indexCompressions: Seq[CompressionInternal],
              valueCompressions: Seq[CompressionInternal],
              falsePositiveRate: Double,
              resetPrefixCompressionEvery: Int,
              minimumNumberOfKeyForHashIndex: Int,
              hashIndexCompensation: Int => Int,
              previous: Option[KeyValue.WriteOnly],
              maxProbe: Int,
              enableRangeFilterAndIndex: Boolean): IO[Option[Transient.Group]] =
      GroupCompressor.compress(
        keyValues = keyValues,
        indexCompressions = indexCompressions,
        valueCompressions = valueCompressions,
        falsePositiveRate = falsePositiveRate,
        resetPrefixCompressionEvery = resetPrefixCompressionEvery,
        hashIndexCompensation = hashIndexCompensation,
        minimumNumberOfKeyForHashIndex = minimumNumberOfKeyForHashIndex,
        previous = previous,
        maxProbe = maxProbe,
        enableRangeFilterAndIndex = enableRangeFilterAndIndex
      )
  }

  case class Group(minKey: Slice[Byte],
                   maxKey: MaxKey[Slice[Byte]],
                   fullKey: Slice[Byte],
                   compressedKeyValues: Slice[Byte],
                   //the deadline is the nearest deadline in the Group's key-values.
                   deadline: Option[Deadline],
                   keyValues: Slice[KeyValue.WriteOnly],
                   previous: Option[KeyValue.WriteOnly],
                   falsePositiveRate: Double,
                   resetPrefixCompressionEvery: Int,
                   minimumNumberOfKeysForHashIndex: Int,
                   hashIndexCompensation: Int => Int,
                   enableRangeFilterAndIndex: Boolean) extends Transient with KeyValue.WriteOnly.Group {

    override def key = minKey

    override val isRemoveRangeMayBe: Boolean = keyValues.last.stats.hasRemoveRange
    override val isRange: Boolean = keyValues.last.stats.hasRange
    override val isGroup: Boolean = true
    override val value: Option[Slice[Byte]] = Some(compressedKeyValues)

    override def updateStats(falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Transient.Group =
      this.copy(falsePositiveRate = falsePositiveRate, previous = previous)

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      KeyValueWriter.write(
        current = this,
        currentTime = Time.empty,
        //it's highly unlikely that 2 groups after compression will have duplicate values.
        //compressDuplicateValues check is unnecessary since the value bytes of a group can be large.
        compressDuplicateValues = false,
        enablePrefixCompression = enablePrefixCompression
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val rangeCommonPrefixesCount: SortedSet[Int] =
      previous map {
        previous =>
          previous.stats.rangeCommonPrefixesCount ++ keyValues.last.stats.rangeCommonPrefixesCount
      } getOrElse keyValues.last.stats.rangeCommonPrefixesCount

    val stats =
      Stats(
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = keyValues.last.stats.hasPut,
        position = previous.map(_.stats.position + 1) getOrElse 1,
        hashIndexItemsCount = previous.map(_.stats.hashIndexItemsCount + keyValues.last.stats.hashIndexItemsCount).getOrElse(keyValues.last.stats.hashIndexItemsCount),
        numberOfRanges = keyValues.last.stats.totalNumberOfRanges,
        bloomFiltersItemCount = keyValues.last.stats.totalBloomFiltersItemsCount,
        usePreviousHashIndexOffset = enablePrefixCompression,
        minimumNumberOfKeysForHashIndex = minimumNumberOfKeysForHashIndex,
        hashIndexCompensation = hashIndexCompensation,
        rangeCommonPrefixesCount = rangeCommonPrefixesCount,
        enableRangeFilterAndIndex = enableRangeFilterAndIndex,
        previous = previous,
        deadline = deadline
      )
  }
}

private[core] sealed trait Persistent extends KeyValue.ReadOnly with KeyValue.CacheAble {

  val indexOffset: Int
  val nextIndexOffset: Int
  val nextIndexSize: Int

  def key: Slice[Byte]

  def isValueDefined: Boolean

  def valueLength: Int

  def valueOffset: Int

  def isPrefixCompressed: Boolean

  /**
    * This function is NOT thread-safe and is mutable. It should always be invoke at the time of creation
    * and before inserting into the Segment's cache.
    */
  def unsliceIndexBytes: Unit
}

private[core] object Persistent {

  sealed trait SegmentResponse extends KeyValue.ReadOnly.SegmentResponse with Persistent {
    def toMemory(): IO[Memory.SegmentResponse]

    def toMemoryResponseOption(): IO[Option[Memory.SegmentResponse]] =
      toMemory() map (Some(_))
  }
  sealed trait Fixed extends Persistent.SegmentResponse with KeyValue.ReadOnly.Fixed

  object Remove {
    def apply(key: Slice[Byte],
              deadline: Option[Deadline],
              time: Time,
              isPrefixCompressed: Boolean): Persistent.Remove =
      Persistent.Remove(
        _key = key,
        deadline = deadline,
        _time = time,
        indexOffset = 0,
        nextIndexOffset = -1,
        nextIndexSize = 0,
        isPrefixCompressed = isPrefixCompressed
      )
  }

  case class Remove(private var _key: Slice[Byte],
                    deadline: Option[Deadline],
                    private var _time: Time,
                    indexOffset: Int,
                    nextIndexOffset: Int,
                    nextIndexSize: Int,
                    isPrefixCompressed: Boolean) extends Persistent.Fixed with KeyValue.ReadOnly.Remove {
    override val valueLength: Int = 0
    override val isValueDefined: Boolean = true
    override val valueOffset: Int = 0

    def key = _key

    def time = _time

    override def indexEntryDeadline: Option[Deadline] = deadline

    override def unsliceIndexBytes(): Unit = {
      _key = _key.unslice()
      _time = _time.unslice()
    }

    def hasTimeLeft(): Boolean =
      deadline.exists(_.hasTimeLeft())

    def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean =
      deadline.exists(deadline => (deadline - minus).hasTimeLeft())

    override def toMemory(): IO[Memory.Remove] =
      IO.Success {
        Memory.Remove(
          key = key,
          deadline = deadline,
          time = time
        )
      }

    override def copyWithTime(time: Time): ReadOnly.Remove =
      copy(_time = time)

    override def toFromValue(): IO[Value.Remove] =
      IO.Success(toRemoveValue())

    override def toRangeValue(): IO[Value.Remove] =
      toFromValue()

    override def toRemoveValue(): Value.Remove =
      Value.Remove(deadline, time)
  }

  object Put {
    def apply(key: Slice[Byte],
              deadline: Option[Deadline],
              time: Time,
              value: Option[Slice[Byte]],
              isPrefixCompressed: Boolean): Persistent.Put =
      Persistent.Put(
        _key = key,
        deadline = deadline,
        lazyValueReader =
          LazyValueReader(
            reader = value.map(Reader(_)).getOrElse(Reader.empty),
            offset = 0,
            length = value.map(_.size).getOrElse(0)
          ),
        _time = time,
        nextIndexOffset = -1,
        nextIndexSize = 0,
        indexOffset = 0,
        valueOffset = 0,
        valueLength = value.map(_.size).getOrElse(0),
        isPrefixCompressed = isPrefixCompressed
      )
  }

  case class Put(private var _key: Slice[Byte],
                 deadline: Option[Deadline],
                 private val lazyValueReader: LazyValueReader,
                 private var _time: Time,
                 nextIndexOffset: Int,
                 nextIndexSize: Int,
                 indexOffset: Int,
                 valueOffset: Int,
                 valueLength: Int,
                 isPrefixCompressed: Boolean) extends Persistent.Fixed with KeyValue.ReadOnly.Put {
    override def unsliceIndexBytes: Unit = {
      _key = _key.unslice()
      _time = _time.unslice()
    }

    override def key: Slice[Byte] =
      _key

    override def time: Time =
      _time

    override def indexEntryDeadline: Option[Deadline] = deadline

    def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean =
      deadline.forall(deadline => (deadline - minus).hasTimeLeft())

    override def getOrFetchValue: IO[Option[Slice[Byte]]] =
      lazyValueReader.getOrFetchValue

    override def isValueDefined: Boolean =
      lazyValueReader.isValueDefined

    override def toFromValue(): IO[Value.Put] =
      getOrFetchValue map {
        value =>
          Value.Put(value, deadline, time)
      }

    override def toRangeValue(): IO[Value.RangeValue] =
      IO.Failure(new Exception("Put cannot be converted to RangeValue"))

    override def toMemory(): IO[Memory.Put] =
      getOrFetchValue map {
        value =>
          Memory.Put(
            key = key,
            value = value,
            deadline = deadline,
            time = time
          )
      }

    override def copyWithDeadlineAndTime(deadline: Option[Deadline],
                                         time: Time): Put =
      copy(deadline = deadline, _time = time)

    override def copyWithTime(time: Time): Put =
      copy(_time = time)
  }

  case class Update(private var _key: Slice[Byte],
                    deadline: Option[Deadline],
                    private val lazyValueReader: LazyValueReader,
                    private var _time: Time,
                    nextIndexOffset: Int,
                    nextIndexSize: Int,
                    indexOffset: Int,
                    valueOffset: Int,
                    valueLength: Int,
                    isPrefixCompressed: Boolean) extends Persistent.Fixed with KeyValue.ReadOnly.Update {
    override def unsliceIndexBytes: Unit = {
      _key = _key.unslice()
      _time = _time.unslice()
    }

    override def key: Slice[Byte] =
      _key

    override def time: Time =
      _time

    override def indexEntryDeadline: Option[Deadline] = deadline

    def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean =
      deadline.forall(deadline => (deadline - minus).hasTimeLeft())

    override def isValueDefined: Boolean =
      lazyValueReader.isValueDefined

    def getOrFetchValue: IO[Option[Slice[Byte]]] =
      lazyValueReader.getOrFetchValue

    override def toFromValue(): IO[Value.Update] =
      getOrFetchValue map {
        value =>
          Value.Update(value, deadline, time)
      }

    override def toRangeValue(): IO[Value.Update] =
      toFromValue()

    override def toMemory(): IO[Memory.Update] =
      getOrFetchValue map {
        value =>
          Memory.Update(
            key = key,
            value = value,
            deadline = deadline,
            time = time
          )
      }

    override def copyWithDeadlineAndTime(deadline: Option[Deadline],
                                         time: Time): Update =
      copy(deadline = deadline, _time = time)

    override def copyWithDeadline(deadline: Option[Deadline]): Update =
      copy(deadline = deadline)

    override def copyWithTime(time: Time): Update =
      copy(_time = time)

    override def toPut(): Persistent.Put =
      Persistent.Put(
        _key = key,
        deadline = deadline,
        lazyValueReader = lazyValueReader,
        _time = time,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        isPrefixCompressed = isPrefixCompressed
      )

    override def toPut(deadline: Option[Deadline]): Persistent.Put =
      Persistent.Put(
        _key = key,
        deadline = deadline,
        lazyValueReader = lazyValueReader,
        _time = time,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        isPrefixCompressed = isPrefixCompressed
      )
  }

  case class Function(private var _key: Slice[Byte],
                      private val lazyFunctionReader: LazyFunctionReader,
                      private var _time: Time,
                      nextIndexOffset: Int,
                      nextIndexSize: Int,
                      indexOffset: Int,
                      valueOffset: Int,
                      valueLength: Int,
                      isPrefixCompressed: Boolean) extends Persistent.Fixed with KeyValue.ReadOnly.Function {
    override def unsliceIndexBytes: Unit = {
      _key = _key.unslice()
      _time = _time.unslice()
    }

    override def key: Slice[Byte] =
      _key

    override def time: Time =
      _time

    override def indexEntryDeadline: Option[Deadline] = None

    override def isValueDefined: Boolean =
      lazyFunctionReader.isValueDefined

    def getOrFetchFunction: IO[Slice[Byte]] =
      lazyFunctionReader.getOrFetchFunction

    override def toFromValue(): IO[Value.Function] =
      getOrFetchFunction map {
        value =>
          Value.Function(value, time)
      }

    override def toRangeValue(): IO[Value.Function] =
      toFromValue()

    override def toMemory(): IO[Memory.Function] =
      getOrFetchFunction map {
        function =>
          Memory.Function(
            key = key,
            function = function,
            time = time
          )
      }

    override def copyWithTime(time: Time): Function =
      copy(_time = time)
  }

  object PendingApply {
    def apply(key: Slice[Byte],
              deadline: Option[Deadline],
              applies: Slice[Value.Apply],
              time: Time,
              isPrefixCompressed: Boolean): Persistent.PendingApply =
      Persistent.PendingApply(
        _key = key,
        _time = time,
        deadline = deadline,
        lazyValueReader = ActivePendingApplyValueReader(applies),
        nextIndexOffset = -1,
        nextIndexSize = 0,
        indexOffset = 0,
        valueOffset = 0,
        valueLength = 0,
        isPrefixCompressed = isPrefixCompressed
      )
  }

  case class PendingApply(private var _key: Slice[Byte],
                          private var _time: Time,
                          deadline: Option[Deadline],
                          lazyValueReader: LazyPendingApplyValueReader,
                          nextIndexOffset: Int,
                          nextIndexSize: Int,
                          indexOffset: Int,
                          valueOffset: Int,
                          valueLength: Int,
                          isPrefixCompressed: Boolean) extends Persistent.Fixed with KeyValue.ReadOnly.PendingApply {
    override def unsliceIndexBytes: Unit = {
      _key = _key.unslice()
      _time = _time.unslice()
    }

    override def key: Slice[Byte] =
      _key

    override def time: Time =
      _time

    override def indexEntryDeadline: Option[Deadline] = deadline

    override def isValueDefined: Boolean =
      lazyValueReader.isValueDefined

    override def getOrFetchApplies: IO[Slice[Value.Apply]] =
      lazyValueReader.getOrFetchApplies

    override def toFromValue(): IO[Value.PendingApply] =
      lazyValueReader.getOrFetchApplies map {
        applies =>
          Value.PendingApply(applies)
      }

    override def toRangeValue(): IO[Value.PendingApply] =
      toFromValue()

    override def toMemory(): IO[Memory.PendingApply] =
      getOrFetchApplies map {
        applies =>
          Memory.PendingApply(
            key = key,
            applies = applies
          )
      }
  }

  object Range {
    def apply(key: Slice[Byte],
              lazyRangeValueReader: LazyRangeValueReader,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              indexOffset: Int,
              valueOffset: Int,
              valueLength: Int,
              isPrefixCompressed: Boolean): IO[Persistent.Range] =
      Bytes.decompressJoin(key) map {
        case (fromKey, toKey) =>
          Range(
            _fromKey = fromKey,
            _toKey = toKey,
            lazyRangeValueReader = lazyRangeValueReader,
            nextIndexOffset = nextIndexOffset,
            nextIndexSize = nextIndexSize,
            indexOffset = indexOffset,
            valueOffset = valueOffset,
            valueLength = valueLength,
            isPrefixCompressed = isPrefixCompressed
          )
      }
  }

  case class Range(private var _fromKey: Slice[Byte],
                   private var _toKey: Slice[Byte],
                   lazyRangeValueReader: LazyRangeValueReader,
                   nextIndexOffset: Int,
                   nextIndexSize: Int,
                   indexOffset: Int,
                   valueOffset: Int,
                   valueLength: Int,
                   isPrefixCompressed: Boolean) extends Persistent.SegmentResponse with KeyValue.ReadOnly.Range {

    def fromKey = _fromKey

    def toKey = _toKey

    override def indexEntryDeadline: Option[Deadline] = None

    override def unsliceIndexBytes: Unit = {
      this._fromKey = _fromKey.unslice()
      this._toKey = _toKey.unslice()
    }

    override def key: Slice[Byte] =
      _fromKey

    override def fetchFromValue: IO[Option[Value.FromValue]] =
      lazyRangeValueReader.fetchFromValue

    override def fetchRangeValue: IO[Value.RangeValue] =
      lazyRangeValueReader.fetchRangeValue

    override def fetchFromAndRangeValue: IO[(Option[Value.FromValue], Value.RangeValue)] =
      lazyRangeValueReader.fetchFromAndRangeValue

    override def isValueDefined: Boolean =
      lazyRangeValueReader.isValueDefined

    override def toMemory(): IO[Memory.Range] =
      fetchFromAndRangeValue map {
        case (fromValue, rangeValue) =>
          Memory.Range(
            fromKey = fromKey,
            toKey = toKey,
            fromValue = fromValue,
            rangeValue = rangeValue
          )
      }
  }

  object Group {
    def apply(key: Slice[Byte],
              valueReader: Reader,
              lazyGroupValueReader: LazyGroupValueReader,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              indexOffset: Int,
              valueLength: Int,
              valueOffset: Int,
              deadline: Option[Deadline],
              isPrefixCompressed: Boolean): IO[Group] =
      GroupKeyCompressor.decompress(key) map {
        case (minKey, maxKey) =>
          Group(
            _minKey = minKey,
            _maxKey = maxKey,
            lazyGroupValueReader = lazyGroupValueReader,
            groupDecompressor = GroupDecompressor(valueReader.copy(), valueOffset),
            valueReader = valueReader.copy(),
            nextIndexOffset = nextIndexOffset,
            nextIndexSize = nextIndexSize,
            indexOffset = indexOffset,
            valueOffset = valueOffset,
            valueLength = valueLength,
            deadline = deadline,
            isPrefixCompressed = isPrefixCompressed
          )
      }
  }

  case class Group(private var _minKey: Slice[Byte],
                   private var _maxKey: MaxKey[Slice[Byte]],
                   private val groupDecompressor: GroupDecompressor,
                   lazyGroupValueReader: LazyGroupValueReader,
                   valueReader: Reader,
                   nextIndexOffset: Int,
                   nextIndexSize: Int,
                   indexOffset: Int,
                   valueOffset: Int,
                   valueLength: Int,
                   deadline: Option[Deadline],
                   isPrefixCompressed: Boolean) extends Persistent with KeyValue.ReadOnly.Group {

    lazy val segmentCacheInitializer =
      new SegmentCacheInitializer(
        id = "Persistent.Group",
        minKey = minKey,
        maxKey = maxKey,
        //persistent key-value's key do not have be sliced either because the decompressed bytes are still in memory.
        //slicing will just use more memory. On memory overflow the Group itself will get dropped and hence all the
        //key-values inside the group's SegmentCache will also be GC'd.
        unsliceKey = false,
        getFooter = groupDecompressor.footer _,
        getHashIndexHeader = groupDecompressor.hashIndexHeader _,
        createReader = groupDecompressor.reader _
      )

    override def indexEntryDeadline: Option[Deadline] = deadline

    override def key: Slice[Byte] =
      _minKey

    override def minKey: Slice[Byte] =
      _minKey

    override def maxKey: MaxKey[Slice[Byte]] =
      _maxKey

    override def unsliceIndexBytes: Unit = {
      this._minKey = _minKey.unslice()
      this._maxKey = _maxKey.unslice()
    }

    def isIndexDecompressed: Boolean =
      groupDecompressor.isIndexDecompressed()

    def isValueDecompressed: Boolean =
      groupDecompressor.isValueDecompressed()

    def isHeaderDecompressed: Boolean =
      groupDecompressor.isHeaderDecompressed()

    def header() =
      groupDecompressor.header()

    /**
      * On uncompressed a new Group is returned. It would be much efficient if the Group's old [[SegmentCache]]'s skipList's
      * key-values are also still passed to the new Group in a thread-safe manner.
      */
    def uncompress(): Persistent.Group =
      copy(groupDecompressor = groupDecompressor.uncompress(), valueReader = valueReader.copy())

    def segmentCache(implicit keyOrder: KeyOrder[Slice[Byte]],
                     keyValueLimiter: KeyValueLimiter): SegmentCache =
      segmentCacheInitializer.segmentCache

    override def isValueDefined: Boolean =
      lazyGroupValueReader.isValueDefined
  }
}
