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

import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.group.compression.data.{GroupHeader, GroupingStrategy}
import swaydb.core.group.compression.{GroupCompressor, GroupDecompressor, GroupKeyCompressor}
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.RangeValueSerializer
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.format.a.{SegmentCompression, SegmentBlock}
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.entry.reader.value._
import swaydb.core.segment.format.a.entry.writer._
import swaydb.core.segment.{BinarySegment, BinarySegmentInitialiser, Segment}
import swaydb.core.util.Bytes
import swaydb.core.util.CollectionUtil._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.{IO, MaxKey}

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
      def segment(implicit keyOrder: KeyOrder[Slice[Byte]],
                  keyValueLimiter: KeyValueLimiter): BinarySegment
      def deadline: Option[Deadline]
    }
  }

  /**
    * Write-only instances are only created after a successful merge of key-values and are used to write to Persistent
    * and Memory Segments.
    */
  sealed trait WriteOnly extends KeyValue { self =>
    val isRemoveRangeMayBe: Boolean
    val isRange: Boolean
    val isGroup: Boolean
    val previous: Option[KeyValue.WriteOnly]
    def valuesConfig: Values.Config
    def sortedIndexConfig: SortedIndex.Config
    def binarySearchIndexConfig: BinarySearchIndex.Config
    def hashIndexConfig: HashIndex.Config
    def bloomFilterConfig: BloomFilter.Config
    def isPrefixCompressed: Boolean
    def fullKey: Slice[Byte]
    def stats: Stats
    def deadline: Option[Deadline]
    def indexEntryBytes: Slice[Byte]
    def valueEntryBytes: Slice[Slice[Byte]]
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

    def updatePrevious(valuesConfig: Values.Config,
                       sortedIndexConfig: SortedIndex.Config,
                       binarySearchIndexConfig: BinarySearchIndex.Config,
                       hashIndexConfig: HashIndex.Config,
                       bloomFilterConfig: BloomFilter.Config,
                       previous: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly

    def reverseIterator: Iterator[WriteOnly] =
      new Iterator[WriteOnly] {
        var currentPrevious: Option[KeyValue.WriteOnly] =
          Some(self)

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
        keyValues.foldLeftWhile(Option.empty[Transient.Group], _.isGroup) {
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

    private lazy val binarySegment: BinarySegmentInitialiser =
      new BinarySegmentInitialiser(
        id = "Persistent.Group",
        minKey = minKey,
        maxKey = maxKey,
        unsliceKey = false,
        createReader = groupDecompressor.reader _
      )

    override def indexEntryDeadline: Option[Deadline] = deadline

    override def key: Slice[Byte] = minKey

    def isValueDefined: Boolean =
      groupDecompressor.isIndexDecompressed()

    def segment(implicit keyOrder: KeyOrder[Slice[Byte]],
                keyValueLimiter: KeyValueLimiter): BinarySegment =
      binarySegment.get

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

  private[core] sealed trait SegmentResponse extends Transient {
    def value: Option[Slice[Byte]]
  }

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
                    valuesConfig: Values.Config,
                    sortedIndexConfig: SortedIndex.Config,
                    binarySearchIndexConfig: BinarySearchIndex.Config,
                    hashIndexConfig: HashIndex.Config,
                    bloomFilterConfig: BloomFilter.Config,
                    previous: Option[KeyValue.WriteOnly]) extends Transient.SegmentResponse with KeyValue.WriteOnly.Fixed {
    override val isRange: Boolean = false
    override val isGroup: Boolean = false
    override val isRemoveRangeMayBe = false
    override val value: Option[Slice[Byte]] = None

    override val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition, isPrefixCompressed) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = false,
        enablePrefixCompression = SortedIndex.Config.enablePrefixCompression(sortedIndexConfig, previous)
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)
    override val stats =
      Stats(
        keySize = key.written,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = false,
        isPrefixCompressed = isPrefixCompressed,
        numberOfRanges = 0,
        thisKeyValuesUniqueKeys = 1,
        sortedIndex = sortedIndexConfig,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previous = previous,
        deadline = deadline
      )

    override def fullKey = key

    override def updatePrevious(valuesConfig: Values.Config,
                                sortedIndexConfig: SortedIndex.Config,
                                binarySearchIndexConfig: BinarySearchIndex.Config,
                                hashIndexConfig: HashIndex.Config,
                                bloomFilterConfig: BloomFilter.Config,
                                previous: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        previous = previous
      )

    override def hasTimeLeft(): Boolean =
      deadline.exists(_.hasTimeLeft())
  }

  case class Put(key: Slice[Byte],
                 value: Option[Slice[Byte]],
                 deadline: Option[Deadline],
                 time: Time,
                 valuesConfig: Values.Config,
                 sortedIndexConfig: SortedIndex.Config,
                 binarySearchIndexConfig: BinarySearchIndex.Config,
                 hashIndexConfig: HashIndex.Config,
                 bloomFilterConfig: BloomFilter.Config,
                 previous: Option[KeyValue.WriteOnly]) extends Transient.SegmentResponse with KeyValue.WriteOnly.Fixed {

    override val isRemoveRangeMayBe = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition, isPrefixCompressed) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = valuesConfig.compressDuplicateValues,
        enablePrefixCompression = SortedIndex.Config.enablePrefixCompression(sortedIndexConfig, previous)
      ).unapply

    override val hasValueEntryBytes: Boolean =
      previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        keySize = key.written,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = true,
        isPrefixCompressed = isPrefixCompressed,
        numberOfRanges = 0,
        thisKeyValuesUniqueKeys = 1,
        sortedIndex = sortedIndexConfig,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previous = previous,
        deadline = deadline
      )

    override def fullKey = key

    override def updatePrevious(valuesConfig: Values.Config,
                                sortedIndexConfig: SortedIndex.Config,
                                binarySearchIndexConfig: BinarySearchIndex.Config,
                                hashIndexConfig: HashIndex.Config,
                                bloomFilterConfig: BloomFilter.Config,
                                previous: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        previous = previous
      )

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())
  }

  case class Update(key: Slice[Byte],
                    value: Option[Slice[Byte]],
                    deadline: Option[Deadline],
                    time: Time,
                    valuesConfig: Values.Config,
                    sortedIndexConfig: SortedIndex.Config,
                    binarySearchIndexConfig: BinarySearchIndex.Config,
                    hashIndexConfig: HashIndex.Config,
                    bloomFilterConfig: BloomFilter.Config,
                    previous: Option[KeyValue.WriteOnly]) extends Transient.SegmentResponse with KeyValue.WriteOnly.Fixed {
    override val isRemoveRangeMayBe = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition, isPrefixCompressed) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = valuesConfig.compressDuplicateValues,
        enablePrefixCompression = SortedIndex.Config.enablePrefixCompression(sortedIndexConfig, previous)
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        keySize = key.written,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = false,
        isPrefixCompressed = isPrefixCompressed,
        numberOfRanges = 0,
        thisKeyValuesUniqueKeys = 1,
        sortedIndex = sortedIndexConfig,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previous = previous,
        deadline = deadline
      )

    override def fullKey = key

    override def updatePrevious(valuesConfig: Values.Config,
                                sortedIndexConfig: SortedIndex.Config,
                                binarySearchIndexConfig: BinarySearchIndex.Config,
                                hashIndexConfig: HashIndex.Config,
                                bloomFilterConfig: BloomFilter.Config,
                                previous: Option[KeyValue.WriteOnly]): Transient.Update =
      this.copy(
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        previous = previous
      )

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())
  }

  case class Function(key: Slice[Byte],
                      function: Slice[Byte],
                      deadline: Option[Deadline],
                      time: Time,
                      valuesConfig: Values.Config,
                      sortedIndexConfig: SortedIndex.Config,
                      binarySearchIndexConfig: BinarySearchIndex.Config,
                      hashIndexConfig: HashIndex.Config,
                      bloomFilterConfig: BloomFilter.Config,
                      previous: Option[KeyValue.WriteOnly]) extends Transient.SegmentResponse with KeyValue.WriteOnly.Fixed {
    override val isRemoveRangeMayBe = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false
    override val value: Option[Slice[Byte]] = Some(function)

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition, isPrefixCompressed) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = valuesConfig.compressDuplicateValues,
        enablePrefixCompression = SortedIndex.Config.enablePrefixCompression(sortedIndexConfig, previous)
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        keySize = key.written,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = false,
        isPrefixCompressed = isPrefixCompressed,
        numberOfRanges = 0,
        thisKeyValuesUniqueKeys = 1,
        sortedIndex = sortedIndexConfig,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previous = previous,
        deadline = deadline
      )

    override def fullKey = key

    override def updatePrevious(valuesConfig: Values.Config,
                                sortedIndexConfig: SortedIndex.Config,
                                binarySearchIndexConfig: BinarySearchIndex.Config,
                                hashIndexConfig: HashIndex.Config,
                                bloomFilterConfig: BloomFilter.Config,
                                previous: Option[KeyValue.WriteOnly]): Transient.Function =
      this.copy(
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        previous = previous
      )

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())
  }

  case class PendingApply(key: Slice[Byte],
                          applies: Slice[Value.Apply],
                          valuesConfig: Values.Config,
                          sortedIndexConfig: SortedIndex.Config,
                          binarySearchIndexConfig: BinarySearchIndex.Config,
                          hashIndexConfig: HashIndex.Config,
                          bloomFilterConfig: BloomFilter.Config,
                          previous: Option[KeyValue.WriteOnly]) extends Transient.SegmentResponse with KeyValue.WriteOnly.Fixed {
    override val isRemoveRangeMayBe = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false
    override val deadline: Option[Deadline] = Segment.getNearestDeadline(None, applies)
    val value: Option[Slice[Byte]] = ??? //Some(ValueSerializer.writeBytes(applies)) TODO - remove values

    override def time = Time.fromApplies(applies)

    override def fullKey = key

    override def updatePrevious(valuesConfig: Values.Config,
                                sortedIndexConfig: SortedIndex.Config,
                                binarySearchIndexConfig: BinarySearchIndex.Config,
                                hashIndexConfig: HashIndex.Config,
                                bloomFilterConfig: BloomFilter.Config,
                                previous: Option[KeyValue.WriteOnly]): Transient.PendingApply =
      this.copy(
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        previous = previous
      )

    override def hasTimeLeft(): Boolean =
      true

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition, isPrefixCompressed) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = valuesConfig.compressDuplicateValues,
        enablePrefixCompression = SortedIndex.Config.enablePrefixCompression(sortedIndexConfig, previous)
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        keySize = key.written,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = false,
        isPrefixCompressed = isPrefixCompressed,
        numberOfRanges = 0,
        thisKeyValuesUniqueKeys = 1,
        sortedIndex = sortedIndexConfig,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previous = previous,
        deadline = deadline
      )
  }

  object Range {

    def apply[R <: Value.RangeValue](fromKey: Slice[Byte],
                                     toKey: Slice[Byte],
                                     rangeValue: R,
                                     valuesConfig: Values.Config,
                                     sortedIndexConfig: SortedIndex.Config,
                                     binarySearchIndexConfig: BinarySearchIndex.Config,
                                     hashIndexConfig: HashIndex.Config,
                                     bloomFilterConfig: BloomFilter.Config,
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
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        previous = previous
      )
    }

    def apply[F <: Value.FromValue, R <: Value.RangeValue](fromKey: Slice[Byte],
                                                           toKey: Slice[Byte],
                                                           fromValue: Option[F],
                                                           rangeValue: R,
                                                           valuesConfig: Values.Config,
                                                           sortedIndexConfig: SortedIndex.Config,
                                                           binarySearchIndexConfig: BinarySearchIndex.Config,
                                                           hashIndexConfig: HashIndex.Config,
                                                           bloomFilterConfig: BloomFilter.Config,
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
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        previous = previous
      )
    }
  }

  case class Range(fromKey: Slice[Byte],
                   toKey: Slice[Byte],
                   fullKey: Slice[Byte],
                   fromValue: Option[Value.FromValue],
                   rangeValue: Value.RangeValue,
                   value: Option[Slice[Byte]],
                   valuesConfig: Values.Config,
                   sortedIndexConfig: SortedIndex.Config,
                   binarySearchIndexConfig: BinarySearchIndex.Config,
                   hashIndexConfig: HashIndex.Config,
                   bloomFilterConfig: BloomFilter.Config,
                   previous: Option[KeyValue.WriteOnly]) extends Transient.SegmentResponse with KeyValue.WriteOnly.Range {

    def key = fromKey

    override val isRemoveRangeMayBe = rangeValue.hasRemoveMayBe
    override val isGroup: Boolean = false
    override val isRange: Boolean = true
    override val deadline: Option[Deadline] = None

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition, isPrefixCompressed) =
      KeyValueWriter.write(
        current = this,
        currentTime = Time.empty,
        //It's highly likely that two sequential key-values within the same range have the different value after the range split occurs so this is always set to true.
        compressDuplicateValues = valuesConfig.compressDuplicateRangeValues,
        enablePrefixCompression = SortedIndex.Config.enablePrefixCompression(sortedIndexConfig, previous)
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val commonBytesCount = Bytes.commonPrefixBytesCount(fromKey, toKey)

    val stats =
      Stats(
        keySize = fromKey.written + toKey.written,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = fromValue.exists(_.isInstanceOf[Value.Put]),
        numberOfRanges = 1,
        thisKeyValuesUniqueKeys = 1,
        sortedIndex = sortedIndexConfig,
        isPrefixCompressed = isPrefixCompressed,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previous = previous,
        deadline = None
      )

    override def updatePrevious(valuesConfig: Values.Config,
                                sortedIndexConfig: SortedIndex.Config,
                                binarySearchIndexConfig: BinarySearchIndex.Config,
                                hashIndexConfig: HashIndex.Config,
                                bloomFilterConfig: BloomFilter.Config,
                                previous: Option[KeyValue.WriteOnly]): Transient.Range =
      this.copy(
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        previous = previous
      )

    override def fetchFromValue: IO[Option[Value.FromValue]] =
      IO.Success(fromValue)

    override def fetchRangeValue: IO[Value.RangeValue] =
      IO.Success(rangeValue)

    override def fetchFromAndRangeValue: IO[(Option[Value.FromValue], Value.RangeValue)] =
      IO.Success(fromValue, rangeValue)
  }

  object Group {

    def apply(keyValues: Slice[KeyValue.WriteOnly],
              previous: Option[KeyValue.WriteOnly],
              //compression is for the group's key-values.
              groupCompression: SegmentCompression,
              //these configs are for the Group itself and not the key-values within the group.
              valuesConfig: Values.Config,
              sortedIndexConfig: SortedIndex.Config,
              binarySearchIndexConfig: BinarySearchIndex.Config,
              hashIndexConfig: HashIndex.Config,
              bloomFilterConfig: BloomFilter.Config): IO[Option[Transient.Group]] =
      GroupCompressor.compress(
        keyValues = keyValues,
        previous = previous,
        segmentCompression = groupCompression,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig
      )
  }

  case class Group(minKey: Slice[Byte],
                   maxKey: MaxKey[Slice[Byte]],
                   fullKey: Slice[Byte],
                   result: SegmentBlock.Result,
                   //the deadline is the nearest deadline in the Group's key-values.
                   deadline: Option[Deadline],
                   keyValues: Slice[KeyValue.WriteOnly],
                   valuesConfig: Values.Config,
                   sortedIndexConfig: SortedIndex.Config,
                   binarySearchIndexConfig: BinarySearchIndex.Config,
                   hashIndexConfig: HashIndex.Config,
                   bloomFilterConfig: BloomFilter.Config,
                   previous: Option[KeyValue.WriteOnly]) extends Transient with KeyValue.WriteOnly.Group {

    override def key = minKey

    override val isRemoveRangeMayBe: Boolean = keyValues.last.stats.segmentHasRemoveRange
    override val isRange: Boolean = keyValues.last.stats.segmentHasRange
    override val isGroup: Boolean = true

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition, isPrefixCompressed) =
      KeyValueWriter.write(
        current = this,
        currentTime = Time.empty,
        //it's highly unlikely that 2 groups after compression will have duplicate values.
        //compressDuplicateValues check is unnecessary since the value bytes of a group can be large.
        compressDuplicateValues = false,
        enablePrefixCompression = SortedIndex.Config.enablePrefixCompression(sortedIndexConfig, previous)
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        keySize = minKey.written + maxKey.maxKey.written,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = keyValues.last.stats.segmentHasPut,
        isPrefixCompressed = isPrefixCompressed,
        numberOfRanges = keyValues.last.stats.segmentTotalNumberOfRanges,
        thisKeyValuesUniqueKeys = keyValues.last.stats.segmentUniqueKeysCount,
        sortedIndex = sortedIndexConfig,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previous = previous,
        deadline = deadline
      )

    override def updatePrevious(valuesConfig: Values.Config,
                                sortedIndexConfig: SortedIndex.Config,
                                binarySearchIndexConfig: BinarySearchIndex.Config,
                                hashIndexConfig: HashIndex.Config,
                                bloomFilterConfig: BloomFilter.Config,
                                previous: Option[KeyValue.WriteOnly]): Transient.Group =
      this.copy(
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        previous = previous
      )
  }
}

private[core] sealed trait Persistent extends KeyValue.CacheAble {

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

    private lazy val binarySegment =
      new BinarySegmentInitialiser(
        id = "Persistent.Group",
        minKey = minKey,
        maxKey = maxKey,
        //persistent key-value's key do not have be sliced either because the decompressed bytes are still in memory.
        //slicing will just use more memory. On memory overflow the Group itself will find dropped and hence all the
        //key-values inside the group's SegmentCache will also be GC'd.
        unsliceKey = false,
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
      * On uncompressed a new Group is returned. It would be much efficient if the Group's old [[BinarySegment]]'s skipList's
      * key-values are also still passed to the new Group in a thread-safe manner.
      */
    def uncompress(): Persistent.Group =
      copy(groupDecompressor = groupDecompressor.uncompress(), valueReader = valueReader.copy())

    def segment(implicit keyOrder: KeyOrder[Slice[Byte]],
                keyValueLimiter: KeyValueLimiter): BinarySegment =
      binarySegment.get

    override def isValueDefined: Boolean =
      lazyGroupValueReader.isValueDefined
  }
}
