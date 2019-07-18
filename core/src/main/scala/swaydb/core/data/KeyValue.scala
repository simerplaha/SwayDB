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
import swaydb.core.group.compression.{GroupCompressor, GroupKeyCompressor}
import swaydb.core.map.serializer.{RangeValueSerializer, ValueSerializer}
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.format.a.block.SegmentBlock.SegmentBlockOps
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.format.a.block.{SegmentBlock, _}
import swaydb.core.segment.format.a.entry.writer._
import swaydb.core.segment.{Segment, SegmentCache}
import swaydb.core.util.CollectionUtil._
import swaydb.core.util.cache.{Cache, CacheNOIO}
import swaydb.core.util.{Bytes, MinMax}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
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
      def segment(implicit keyOrder: KeyOrder[Slice[Byte]],
                  keyValueLimiter: KeyValueLimiter,
                  groupIO: SegmentIO): SegmentCache
      def deadline: Option[Deadline]
      def isCached: Boolean
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
              blockedSegment: SegmentBlock.Closed): Group =
      Group(
        minKey = minKey.unslice(),
        maxKey = maxKey.unslice(),
        segmentBytes = blockedSegment.flattenSegmentBytes.unslice(),
        deadline = blockedSegment.nearestDeadline
      )
  }

  case class Group(minKey: Slice[Byte],
                   maxKey: MaxKey[Slice[Byte]],
                   segmentBytes: Slice[Byte],
                   deadline: Option[Deadline]) extends Memory with KeyValue.ReadOnly.Group {

    private val segmentCache: CacheNOIO[(KeyOrder[Slice[Byte]], KeyValueLimiter, SegmentIO), SegmentCache] =
      Cache.noIO(synchronised = true, stored = true) {
        case (keyOrder: KeyOrder[Slice[Byte]], limiter: KeyValueLimiter, groupIO: SegmentIO) =>
          SegmentCache(
            id = "Memory.Group - BinarySegment",
            maxKey = maxKey,
            minKey = minKey,
            unsliceKey = false,
            blockRef = BlockRefReader(segmentBytes)(SegmentBlockOps),
            segmentIO = groupIO
          )(keyOrder, limiter)
      }

    override def valueLength: Int = segmentBytes.size

    override def indexEntryDeadline: Option[Deadline] = deadline

    override def key: Slice[Byte] = minKey

    override def isCached: Boolean =
      segmentCache.isCached

    def segment(implicit keyOrder: KeyOrder[Slice[Byte]],
                keyValueLimiter: KeyValueLimiter,
                config: SegmentIO): SegmentCache =
      segmentCache getOrElse {
        segmentCache.value(keyOrder, keyValueLimiter, config)
      }

    def uncompress(): Memory.Group = {
      segmentCache.clear()
      this
    }
  }
}

private[core] sealed trait Transient extends KeyValue { self =>
  val isRemoveRangeMayBe: Boolean
  val isRange: Boolean
  val isGroup: Boolean
  val previous: Option[Transient]
  def minKey: Slice[Byte]
  def values: Slice[Slice[Byte]]
  def valuesConfig: ValuesBlock.Config
  def sortedIndexConfig: SortedIndexBlock.Config
  def binarySearchIndexConfig: BinarySearchIndexBlock.Config
  def hashIndexConfig: HashIndexBlock.Config
  def bloomFilterConfig: BloomFilterBlock.Config
  def isPrefixCompressed: Boolean
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

  def updatePrevious(valuesConfig: ValuesBlock.Config,
                     sortedIndexConfig: SortedIndexBlock.Config,
                     binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                     hashIndexConfig: HashIndexBlock.Config,
                     bloomFilterConfig: BloomFilterBlock.Config,
                     previous: Option[Transient]): Transient

  def reverseIterator: Iterator[Transient] =
    new Iterator[Transient] {
      var currentPrevious: Option[Transient] =
        Some(self)

      override def hasNext: Boolean =
        currentPrevious.isDefined

      override def next(): Transient = {
        val next = currentPrevious.get
        currentPrevious = next.previous
        next
      }
    }
}

private[core] object Transient {

  implicit class TransientIterableImplicits(keyValues: Iterable[Transient]) {
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

  sealed trait Fixed extends Transient {

    def hasTimeLeft(): Boolean
    def isOverdue(): Boolean = !hasTimeLeft()
    def time: Time
  }

  def hasSameValue(left: Transient, right: Transient): Boolean =
    (left, right) match {
      //Groups
      case (_: Transient.Group, right: Transient) => false
      case (_: Transient, right: Transient.Group) => false
      //Remove
      case (left: Transient.Remove, right: Transient.Remove) => true
      case (left: Transient.Remove, right: Transient.Put) => right.value.isEmpty
      case (left: Transient.Remove, right: Transient.Update) => right.value.isEmpty
      case (left: Transient.Remove, right: Transient.Function) => false
      case (left: Transient.Remove, right: Transient.PendingApply) => false
      case (left: Transient.Remove, right: Transient.Range) => false
      //Put
      case (left: Transient.Put, right: Transient.Remove) => left.value.isEmpty
      case (left: Transient.Put, right: Transient.Put) => left.value == right.value
      case (left: Transient.Put, right: Transient.Update) => left.value == right.value
      case (left: Transient.Put, right: Transient.Function) => left.value contains right.function
      case (left: Transient.Put, right: Transient.PendingApply) => false
      case (left: Transient.Put, right: Transient.Range) => false
      //Update
      case (left: Transient.Update, right: Transient.Remove) => left.value.isEmpty
      case (left: Transient.Update, right: Transient.Put) => left.value == right.value
      case (left: Transient.Update, right: Transient.Update) => left.value == right.value
      case (left: Transient.Update, right: Transient.Function) => left.value contains right.function
      case (left: Transient.Update, right: Transient.PendingApply) => false
      case (left: Transient.Update, right: Transient.Range) => false
      //Function
      case (left: Transient.Function, right: Transient.Remove) => false
      case (left: Transient.Function, right: Transient.Put) => right.value contains left.function
      case (left: Transient.Function, right: Transient.Update) => right.value contains left.function
      case (left: Transient.Function, right: Transient.Function) => left.function == right.function
      case (left: Transient.Function, right: Transient.PendingApply) => false
      case (left: Transient.Function, right: Transient.Range) => false
      //PendingApply
      case (left: Transient.PendingApply, right: Transient.Remove) => false
      case (left: Transient.PendingApply, right: Transient.Put) => false
      case (left: Transient.PendingApply, right: Transient.Update) => false
      case (left: Transient.PendingApply, right: Transient.Function) => false
      case (left: Transient.PendingApply, right: Transient.PendingApply) => left.applies == right.applies
      case (left: Transient.PendingApply, right: Transient.Range) => false
      //Range
      case (left: Transient.Range, right: Transient.Remove) => false
      case (left: Transient.Range, right: Transient.Put) => false
      case (left: Transient.Range, right: Transient.Update) => false
      case (left: Transient.Range, right: Transient.Function) => false
      case (left: Transient.Range, right: Transient.PendingApply) => false
      case (left: Transient.Range, right: Transient.Range) => left.fromValue == right.fromValue && left.rangeValue == right.rangeValue
    }

  //do not fetch the value itself as it will be serialised if it is a range.
  //Here we just check the types to determine if a key-value has value.
  def hasValue(keyValue: Transient): Boolean =
    keyValue match {
      case transient: Transient.Put =>
        transient.value.exists(_.nonEmpty)

      case transient: Transient.Update =>
        transient.value.exists(_.nonEmpty)

      case _: Transient.Remove =>
        false

      case _: Transient.Group | _: Transient.Range | _: Transient.PendingApply | _: Transient.Function =>
        true
    }

  def hasNoValue(keyValue: Transient): Boolean =
    !hasValue(keyValue)

  def compressibleValue(keyValue: Transient): Option[Slice[Byte]] =
    keyValue match {
      case transient: Transient.SegmentResponse =>
        //if value is empty byte slice, return None instead of empty Slice.We do not store empty byte arrays.
        if (transient.value.exists(_.isEmpty))
          None
        else
          transient.value
      case _: Transient.Group =>
        None
    }

  def enablePrefixCompression(keyValue: Transient): Boolean =
    keyValue.sortedIndexConfig.prefixCompressionResetCount > 0 &&
      keyValue.previous.exists {
        previous =>
          (previous.stats.chainPosition + 1) % keyValue.sortedIndexConfig.prefixCompressionResetCount != 0
      }

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
                    valuesConfig: ValuesBlock.Config,
                    sortedIndexConfig: SortedIndexBlock.Config,
                    binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                    hashIndexConfig: HashIndexBlock.Config,
                    bloomFilterConfig: BloomFilterBlock.Config,
                    previous: Option[Transient]) extends Transient.SegmentResponse with Transient.Fixed {
    override val isRange: Boolean = false
    override val isGroup: Boolean = false
    override val isRemoveRangeMayBe = false
    override def minKey = key
    override def value: Option[Slice[Byte]] = None
    override def values: Slice[Slice[Byte]] = Slice.emptyEmptyBytes

    override val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition, isPrefixCompressed) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = false,
        enablePrefixCompression = Transient.enablePrefixCompression(this)
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)
    override val stats =
      Stats(
        keySize = key.size,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = false,
        isPrefixCompressed = isPrefixCompressed,
        thisKeyValuesNumberOfRanges = 0,
        thisKeyValuesUniqueKeys = 1,
        sortedIndex = sortedIndexConfig,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previousStats = previous.map(_.stats),
        deadline = deadline
      )

    override def updatePrevious(valuesConfig: ValuesBlock.Config,
                                sortedIndexConfig: SortedIndexBlock.Config,
                                binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                hashIndexConfig: HashIndexBlock.Config,
                                bloomFilterConfig: BloomFilterBlock.Config,
                                previous: Option[Transient]): Transient =
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
                 valuesConfig: ValuesBlock.Config,
                 sortedIndexConfig: SortedIndexBlock.Config,
                 binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                 hashIndexConfig: HashIndexBlock.Config,
                 bloomFilterConfig: BloomFilterBlock.Config,
                 previous: Option[Transient]) extends Transient.SegmentResponse with Transient.Fixed {

    override val isRemoveRangeMayBe = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false
    override def minKey = key
    override def values: Slice[Slice[Byte]] = value.map(Slice(_)) getOrElse Slice.emptyEmptyBytes

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition, isPrefixCompressed) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = valuesConfig.compressDuplicateValues,
        enablePrefixCompression = Transient.enablePrefixCompression(this)
      ).unapply

    override val hasValueEntryBytes: Boolean =
      previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        keySize = key.size,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = true,
        isPrefixCompressed = isPrefixCompressed,
        thisKeyValuesNumberOfRanges = 0,
        thisKeyValuesUniqueKeys = 1,
        sortedIndex = sortedIndexConfig,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previousStats = previous.map(_.stats),
        deadline = deadline
      )

    override def updatePrevious(valuesConfig: ValuesBlock.Config,
                                sortedIndexConfig: SortedIndexBlock.Config,
                                binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                hashIndexConfig: HashIndexBlock.Config,
                                bloomFilterConfig: BloomFilterBlock.Config,
                                previous: Option[Transient]): Transient =
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
                    valuesConfig: ValuesBlock.Config,
                    sortedIndexConfig: SortedIndexBlock.Config,
                    binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                    hashIndexConfig: HashIndexBlock.Config,
                    bloomFilterConfig: BloomFilterBlock.Config,
                    previous: Option[Transient]) extends Transient.SegmentResponse with Transient.Fixed {
    override val isRemoveRangeMayBe = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false
    override def minKey = key
    override def values: Slice[Slice[Byte]] = value.map(Slice(_)) getOrElse Slice.emptyEmptyBytes

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition, isPrefixCompressed) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = valuesConfig.compressDuplicateValues,
        enablePrefixCompression = Transient.enablePrefixCompression(this)
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        keySize = key.size,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = false,
        isPrefixCompressed = isPrefixCompressed,
        thisKeyValuesNumberOfRanges = 0,
        thisKeyValuesUniqueKeys = 1,
        sortedIndex = sortedIndexConfig,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previousStats = previous.map(_.stats),
        deadline = deadline
      )

    override def updatePrevious(valuesConfig: ValuesBlock.Config,
                                sortedIndexConfig: SortedIndexBlock.Config,
                                binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                hashIndexConfig: HashIndexBlock.Config,
                                bloomFilterConfig: BloomFilterBlock.Config,
                                previous: Option[Transient]): Transient.Update =
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
                      time: Time,
                      valuesConfig: ValuesBlock.Config,
                      sortedIndexConfig: SortedIndexBlock.Config,
                      binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                      hashIndexConfig: HashIndexBlock.Config,
                      bloomFilterConfig: BloomFilterBlock.Config,
                      previous: Option[Transient]) extends Transient.SegmentResponse with Transient.Fixed {
    override val isRemoveRangeMayBe = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false
    override def minKey = key
    override def value: Option[Slice[Byte]] = Some(function)
    override def values: Slice[Slice[Byte]] = Slice(function)
    override def deadline: Option[Deadline] = None

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition, isPrefixCompressed) =
      KeyValueWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = valuesConfig.compressDuplicateValues,
        enablePrefixCompression = Transient.enablePrefixCompression(this)
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        keySize = key.size,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = false,
        isPrefixCompressed = isPrefixCompressed,
        thisKeyValuesNumberOfRanges = 0,
        thisKeyValuesUniqueKeys = 1,
        sortedIndex = sortedIndexConfig,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previousStats = previous.map(_.stats),
        deadline = deadline
      )

    override def updatePrevious(valuesConfig: ValuesBlock.Config,
                                sortedIndexConfig: SortedIndexBlock.Config,
                                binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                hashIndexConfig: HashIndexBlock.Config,
                                bloomFilterConfig: BloomFilterBlock.Config,
                                previous: Option[Transient]): Transient.Function =
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
                          valuesConfig: ValuesBlock.Config,
                          sortedIndexConfig: SortedIndexBlock.Config,
                          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                          hashIndexConfig: HashIndexBlock.Config,
                          bloomFilterConfig: BloomFilterBlock.Config,
                          previous: Option[Transient]) extends Transient.SegmentResponse with Transient.Fixed {
    override val isRemoveRangeMayBe = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false
    override def minKey = key
    override val deadline: Option[Deadline] = Segment.getNearestDeadline(None, applies)
    override val value: Option[Slice[Byte]] = Some(ValueSerializer.writeBytes(applies))
    override def values: Slice[Slice[Byte]] = value.map(Slice(_)) getOrElse Slice.emptyEmptyBytes

    override def time = Time.fromApplies(applies)

    override def updatePrevious(valuesConfig: ValuesBlock.Config,
                                sortedIndexConfig: SortedIndexBlock.Config,
                                binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                hashIndexConfig: HashIndexBlock.Config,
                                bloomFilterConfig: BloomFilterBlock.Config,
                                previous: Option[Transient]): Transient.PendingApply =
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
        enablePrefixCompression = Transient.enablePrefixCompression(this)
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        keySize = key.size,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = false,
        isPrefixCompressed = isPrefixCompressed,
        thisKeyValuesNumberOfRanges = 0,
        thisKeyValuesUniqueKeys = 1,
        sortedIndex = sortedIndexConfig,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previousStats = previous.map(_.stats),
        deadline = deadline
      )
  }

  object Range {

    def apply[R <: Value.RangeValue](fromKey: Slice[Byte],
                                     toKey: Slice[Byte],
                                     rangeValue: R,
                                     valuesConfig: ValuesBlock.Config,
                                     sortedIndexConfig: SortedIndexBlock.Config,
                                     binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                     hashIndexConfig: HashIndexBlock.Config,
                                     bloomFilterConfig: BloomFilterBlock.Config,
                                     previous: Option[Transient])(implicit rangeValueSerializer: RangeValueSerializer[Unit, R]): Range = {

      def valueSerialiser() = {
        val bytesRequired = rangeValueSerializer.bytesRequired((), rangeValue)
        val bytes = if (bytesRequired == 0) None else Some(Slice.create[Byte](bytesRequired))
        bytes.foreach(rangeValueSerializer.write((), rangeValue, _))
        bytes
      }

      val mergedKey = Bytes.compressJoin(fromKey, toKey)
      new Range(
        fromKey = fromKey,
        toKey = toKey,
        key = mergedKey,
        fromValue = None,
        rangeValue = rangeValue,
        valueSerialiser = valueSerialiser _,
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
                                                           valuesConfig: ValuesBlock.Config,
                                                           sortedIndexConfig: SortedIndexBlock.Config,
                                                           binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                                           hashIndexConfig: HashIndexBlock.Config,
                                                           bloomFilterConfig: BloomFilterBlock.Config,
                                                           previous: Option[Transient])(implicit rangeValueSerializer: RangeValueSerializer[Option[F], R]): Range = {
      def valueSerialiser() = {
        val bytesRequired = rangeValueSerializer.bytesRequired(fromValue, rangeValue)
        val bytes = if (bytesRequired == 0) None else Some(Slice.create[Byte](bytesRequired))
        bytes.foreach(rangeValueSerializer.write(fromValue, rangeValue, _))
        bytes
      }

      val mergedKey: Slice[Byte] = Bytes.compressJoin(fromKey, toKey)

      new Range(
        fromKey = fromKey,
        toKey = toKey,
        key = mergedKey,
        fromValue = fromValue,
        rangeValue = rangeValue,
        valueSerialiser = valueSerialiser _,
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
                   key: Slice[Byte],
                   fromValue: Option[Value.FromValue],
                   rangeValue: Value.RangeValue,
                   valueSerialiser: () => Option[Slice[Byte]],
                   valuesConfig: ValuesBlock.Config,
                   sortedIndexConfig: SortedIndexBlock.Config,
                   binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                   hashIndexConfig: HashIndexBlock.Config,
                   bloomFilterConfig: BloomFilterBlock.Config,
                   previous: Option[Transient]) extends Transient.SegmentResponse {

    override val isRemoveRangeMayBe = rangeValue.hasRemoveMayBe
    override val isGroup: Boolean = false
    override val isRange: Boolean = true
    override val deadline: Option[Deadline] = None
    override def minKey = fromKey
    override def value = valueSerialiser()
    override def values: Slice[Slice[Byte]] = value.map(Slice(_)) getOrElse Slice.emptyEmptyBytes

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition, isPrefixCompressed) =
      KeyValueWriter.write(
        current = this,
        currentTime = Time.empty,
        //It's highly likely that two sequential key-values within the same range have the different value after the range split occurs so this is always set to true.
        compressDuplicateValues = valuesConfig.compressDuplicateRangeValues,
        enablePrefixCompression = Transient.enablePrefixCompression(this)
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        keySize = fromKey.size + toKey.size,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = fromValue.exists(_.isInstanceOf[Value.Put]),
        thisKeyValuesNumberOfRanges = 1,
        thisKeyValuesUniqueKeys = 1,
        sortedIndex = sortedIndexConfig,
        isPrefixCompressed = isPrefixCompressed,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previousStats = previous.map(_.stats),
        deadline = None
      )

    override def updatePrevious(valuesConfig: ValuesBlock.Config,
                                sortedIndexConfig: SortedIndexBlock.Config,
                                binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                hashIndexConfig: HashIndexBlock.Config,
                                bloomFilterConfig: BloomFilterBlock.Config,
                                previous: Option[Transient]): Transient.Range =
      this.copy(
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        previous = previous
      )
  }

  object Group {

    def apply(keyValues: Slice[Transient],
              previous: Option[Transient],
              //compression is for the group's key-values.
              groupConfig: SegmentBlock.Config,
              //these configs are for the Group itself and not the key-values within the group.
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config): IO[Transient.Group] =
      GroupCompressor.compress(
        keyValues = keyValues,
        previous = previous,
        groupConfig = groupConfig,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig
      )
  }

  case class Group(minKey: Slice[Byte],
                   maxKey: MaxKey[Slice[Byte]],
                   key: Slice[Byte],
                   blockedSegment: SegmentBlock.Closed,
                   //the deadline is the nearest deadline in the Group's key-values.
                   minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                   deadline: Option[Deadline],
                   keyValues: Slice[Transient],
                   valuesConfig: ValuesBlock.Config,
                   sortedIndexConfig: SortedIndexBlock.Config,
                   binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                   hashIndexConfig: HashIndexBlock.Config,
                   bloomFilterConfig: BloomFilterBlock.Config,
                   previous: Option[Transient]) extends Transient {

    override val isRemoveRangeMayBe: Boolean = keyValues.last.stats.segmentHasRemoveRange
    override val isRange: Boolean = keyValues.last.stats.segmentHasRange
    override val isGroup: Boolean = true
    override def values: Slice[Slice[Byte]] = blockedSegment.segmentBytes

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition, isPrefixCompressed) =
      KeyValueWriter.write(
        current = this,
        currentTime = Time.empty,
        //it's highly unlikely that 2 groups after compression will have duplicate values.
        //compressDuplicateValues check is unnecessary since the value bytes of a group can be large.
        compressDuplicateValues = false,
        enablePrefixCompression = Transient.enablePrefixCompression(this)
      ).unapply

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        keySize = minKey.size + maxKey.maxKey.size,
        indexEntry = indexEntryBytes,
        value = valueEntryBytes,
        isRemoveRange = isRemoveRangeMayBe,
        isRange = isRange,
        isGroup = isGroup,
        isPut = keyValues.last.stats.segmentHasPut,
        isPrefixCompressed = isPrefixCompressed,
        thisKeyValuesNumberOfRanges = keyValues.last.stats.segmentTotalNumberOfRanges,
        thisKeyValuesUniqueKeys = keyValues.last.stats.segmentUniqueKeysCount,
        sortedIndex = sortedIndexConfig,
        bloomFilter = bloomFilterConfig,
        hashIndex = hashIndexConfig,
        binarySearch = binarySearchIndexConfig,
        values = valuesConfig,
        previousStats = previous.map(_.stats),
        deadline = deadline
      )

    override def updatePrevious(valuesConfig: ValuesBlock.Config,
                                sortedIndexConfig: SortedIndexBlock.Config,
                                binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                hashIndexConfig: HashIndexBlock.Config,
                                bloomFilterConfig: BloomFilterBlock.Config,
                                previous: Option[Transient]): Transient.Group =
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
  val accessPosition: Int

  def key: Slice[Byte]

  def valueLength: Int

  def valueOffset: Int

  def isPrefixCompressed: Boolean

  /**
    * This function is NOT thread-safe and is mutable. It should always be invoke at the time of creation
    * and before inserting into the Segment's cache.
    */
  def unsliceKeys: Unit
}

private[core] object Persistent {

  sealed trait SegmentResponse extends KeyValue.ReadOnly.SegmentResponse with Persistent {
    def toMemory(): IO[Memory.SegmentResponse]

    def isValueDefined: Boolean

    def toMemoryResponseOption(): IO[Option[Memory.SegmentResponse]] =
      toMemory() map (Some(_))
  }
  sealed trait Fixed extends Persistent.SegmentResponse with KeyValue.ReadOnly.Fixed

  case class Remove(private var _key: Slice[Byte],
                    deadline: Option[Deadline],
                    private var _time: Time,
                    indexOffset: Int,
                    nextIndexOffset: Int,
                    nextIndexSize: Int,
                    accessPosition: Int,
                    isPrefixCompressed: Boolean) extends Persistent.Fixed with KeyValue.ReadOnly.Remove {
    override val valueLength: Int = 0
    override val isValueDefined: Boolean = true
    override val valueOffset: Int = 0

    def key = _key

    def time = _time

    override def indexEntryDeadline: Option[Deadline] = deadline

    override def unsliceKeys(): Unit = {
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
    def fromCache(key: Slice[Byte],
                  deadline: Option[Deadline],
                  valueCache: Cache[ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                  time: Time,
                  nextIndexOffset: Int,
                  nextIndexSize: Int,
                  indexOffset: Int,
                  valueOffset: Int,
                  valueLength: Int,
                  accessPosition: Int,
                  isPrefixCompressed: Boolean) =
      new Put(
        _key = key,
        deadline = deadline,
        valueCache =
          valueCache mapStored {
            reader =>
              reader.readAllOrNone()
          },
        _time = time,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        accessPosition = accessPosition,
        isPrefixCompressed = isPrefixCompressed
      )
  }

  case class Put(private var _key: Slice[Byte],
                 deadline: Option[Deadline],
                 private val valueCache: Cache[ValuesBlock.Offset, Option[Slice[Byte]]],
                 private var _time: Time,
                 nextIndexOffset: Int,
                 nextIndexSize: Int,
                 indexOffset: Int,
                 valueOffset: Int,
                 valueLength: Int,
                 accessPosition: Int,
                 isPrefixCompressed: Boolean) extends Persistent.Fixed with KeyValue.ReadOnly.Put {
    override def unsliceKeys: Unit = {
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
      valueCache.value(ValuesBlock.Offset(valueOffset, valueLength))

    override def isValueDefined: Boolean =
      valueCache.isCached

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

  object Update {
    def fromCache(key: Slice[Byte],
                  deadline: Option[Deadline],
                  valueCache: Cache[ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                  time: Time,
                  nextIndexOffset: Int,
                  nextIndexSize: Int,
                  indexOffset: Int,
                  valueOffset: Int,
                  valueLength: Int,
                  accessPosition: Int,
                  isPrefixCompressed: Boolean) =
      new Update(
        _key = key,
        deadline = deadline,
        valueCache =
          valueCache mapStored {
            reader =>
              reader.readAllOrNone()
          },
        _time = time,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        accessPosition = accessPosition,
        isPrefixCompressed = isPrefixCompressed
      )
  }

  case class Update(private var _key: Slice[Byte],
                    deadline: Option[Deadline],
                    private val valueCache: Cache[ValuesBlock.Offset, Option[Slice[Byte]]],
                    private var _time: Time,
                    nextIndexOffset: Int,
                    nextIndexSize: Int,
                    indexOffset: Int,
                    valueOffset: Int,
                    valueLength: Int,
                    accessPosition: Int,
                    isPrefixCompressed: Boolean) extends Persistent.Fixed with KeyValue.ReadOnly.Update {
    override def unsliceKeys: Unit = {
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
      valueCache.isCached

    def getOrFetchValue: IO[Option[Slice[Byte]]] =
      valueCache.value(ValuesBlock.Offset(valueOffset, valueLength))

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
        valueCache = valueCache,
        _time = time,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        accessPosition = accessPosition,
        isPrefixCompressed = isPrefixCompressed
      )

    override def toPut(deadline: Option[Deadline]): Persistent.Put =
      Persistent.Put(
        _key = key,
        deadline = deadline,
        valueCache = valueCache,
        _time = time,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        accessPosition = accessPosition,
        isPrefixCompressed = isPrefixCompressed
      )
  }

  object Function {
    def fromCache(key: Slice[Byte],
                  valueCache: Cache[ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                  time: Time,
                  nextIndexOffset: Int,
                  nextIndexSize: Int,
                  indexOffset: Int,
                  valueOffset: Int,
                  valueLength: Int,
                  accessPosition: Int,
                  isPrefixCompressed: Boolean) =
      new Function(
        _key = key,
        valueCache = valueCache.mapStored(_.readAll()),
        _time = time,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        accessPosition = accessPosition,
        isPrefixCompressed = isPrefixCompressed
      )
  }

  case class Function(private var _key: Slice[Byte],
                      private val valueCache: Cache[ValuesBlock.Offset, Slice[Byte]],
                      private var _time: Time,
                      nextIndexOffset: Int,
                      nextIndexSize: Int,
                      indexOffset: Int,
                      valueOffset: Int,
                      valueLength: Int,
                      accessPosition: Int,
                      isPrefixCompressed: Boolean) extends Persistent.Fixed with KeyValue.ReadOnly.Function {
    override def unsliceKeys: Unit = {
      _key = _key.unslice()
      _time = _time.unslice()
    }

    override def key: Slice[Byte] =
      _key

    override def time: Time =
      _time

    override def indexEntryDeadline: Option[Deadline] = None

    override def isValueDefined: Boolean =
      valueCache.isCached

    def getOrFetchFunction: IO[Slice[Byte]] =
      valueCache.value(ValuesBlock.Offset(valueOffset, valueLength))

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
    def fromCache(key: Slice[Byte],
                  time: Time,
                  deadline: Option[Deadline],
                  valueCache: Cache[ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                  nextIndexOffset: Int,
                  nextIndexSize: Int,
                  indexOffset: Int,
                  valueOffset: Int,
                  valueLength: Int,
                  accessPosition: Int,
                  isPrefixCompressed: Boolean) =
      new PendingApply(
        _key = key,
        _time = time,
        deadline = deadline,
        valueCache = valueCache mapStored {
          reader =>
            reader
              .readAll()
              .flatMap(bytes => ValueSerializer.read[Slice[Value.Apply]](bytes))
        },
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        accessPosition = accessPosition,
        isPrefixCompressed = isPrefixCompressed
      )
  }

  case class PendingApply(private var _key: Slice[Byte],
                          private var _time: Time,
                          deadline: Option[Deadline],
                          valueCache: Cache[ValuesBlock.Offset, Slice[Value.Apply]],
                          nextIndexOffset: Int,
                          nextIndexSize: Int,
                          indexOffset: Int,
                          valueOffset: Int,
                          valueLength: Int,
                          accessPosition: Int,
                          isPrefixCompressed: Boolean) extends Persistent.Fixed with KeyValue.ReadOnly.PendingApply {
    override def unsliceKeys: Unit = {
      _key = _key.unslice()
      _time = _time.unslice()
    }

    override def key: Slice[Byte] =
      _key

    override def time: Time =
      _time

    override def indexEntryDeadline: Option[Deadline] = deadline

    override def isValueDefined: Boolean =
      valueCache.isCached

    override def getOrFetchApplies: IO[Slice[Value.Apply]] =
      valueCache.value(ValuesBlock.Offset(valueOffset, valueLength))

    override def toFromValue(): IO[Value.PendingApply] =
      valueCache
        .value(ValuesBlock.Offset(valueOffset, valueLength))
        .map(Value.PendingApply)

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
              valueCache: Cache[ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
              nextIndexOffset: Int,
              nextIndexSize: Int,
              indexOffset: Int,
              valueOffset: Int,
              valueLength: Int,
              accessPosition: Int,
              isPrefixCompressed: Boolean): IO[Persistent.Range] =
      Bytes.decompressJoin(key) map {
        case (fromKey, toKey) =>
          Range(
            _fromKey = fromKey,
            _toKey = toKey,
            valueCache = valueCache mapStored {
              rangeReader =>
                rangeReader.readAll().flatMap(RangeValueSerializer.read)
            },
            nextIndexOffset = nextIndexOffset,
            nextIndexSize = nextIndexSize,
            indexOffset = indexOffset,
            valueOffset = valueOffset,
            valueLength = valueLength,
            accessPosition = accessPosition,
            isPrefixCompressed = isPrefixCompressed
          )
      }
  }

  case class Range(private var _fromKey: Slice[Byte],
                   private var _toKey: Slice[Byte],
                   valueCache: Cache[ValuesBlock.Offset, (Option[Value.FromValue], Value.RangeValue)],
                   nextIndexOffset: Int,
                   nextIndexSize: Int,
                   indexOffset: Int,
                   valueOffset: Int,
                   valueLength: Int,
                   accessPosition: Int,
                   isPrefixCompressed: Boolean) extends Persistent.SegmentResponse with KeyValue.ReadOnly.Range {

    def fromKey = _fromKey

    def toKey = _toKey

    override def indexEntryDeadline: Option[Deadline] = None

    override def unsliceKeys: Unit = {
      this._fromKey = _fromKey.unslice()
      this._toKey = _toKey.unslice()
    }

    override def key: Slice[Byte] =
      _fromKey

    def fetchRangeValue: IO[Value.RangeValue] =
      fetchFromAndRangeValue.map(_._2)

    def fetchFromValue: IO[Option[Value.FromValue]] =
      fetchFromAndRangeValue.map(_._1)

    def fetchFromAndRangeValue: IO[(Option[Value.FromValue], Value.RangeValue)] =
      valueCache.value(ValuesBlock.Offset(valueOffset, valueLength))

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

    override def isValueDefined: Boolean =
      valueCache.isCached
  }

  object Group {
    def apply(key: Slice[Byte],
              valueCache: Cache[ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
              nextIndexOffset: Int,
              nextIndexSize: Int,
              indexOffset: Int,
              valueLength: Int,
              valueOffset: Int,
              accessPosition: Int,
              deadline: Option[Deadline],
              isPrefixCompressed: Boolean): IO[Group] =
      GroupKeyCompressor.decompress(key) flatMap {
        case (minKey, maxKey) =>
          valueCache.value(ValuesBlock.Offset(valueOffset, valueLength)) map {
            reader =>
              val segmentCache: CacheNOIO[(KeyOrder[Slice[Byte]], KeyValueLimiter, SegmentIO), SegmentCache] =
                Cache.noIO(synchronised = true, stored = true) {
                  case (keyOrder: KeyOrder[Slice[Byte]], limiter: KeyValueLimiter, groupIO: SegmentIO) =>
                    val moved: BlockRefReader[SegmentBlock.Offset] =
                      BlockRefReader.moveTo(
                        SegmentBlock.Offset(
                          //cache will return a reader with the offset pointing to this Group's offset, here simply reset to return as an BlockRef within the parent Segment's values block.
                          start = 0,
                          size = valueLength
                        ),
                        reader = reader
                      )

                    SegmentCache(
                      id = "Persistent.Group - BinarySegment",
                      maxKey = maxKey,
                      minKey = minKey,
                      //persistent key-value's key do not have be sliced either because the decompressed bytes are still in memory.
                      //slicing will just use more memory. On memory overflow the Group itself will find dropped and hence all the
                      //key-values inside the group's SegmentCache will also be GC'd.
                      unsliceKey = false,
                      blockRef = moved,
                      segmentIO = groupIO
                    )(keyOrder, limiter)
                }

              Group(
                _minKey = minKey,
                _maxKey = maxKey,
                segmentCache = segmentCache,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                indexOffset = indexOffset,
                valueOffset = valueOffset,
                valueLength = valueLength,
                accessPosition = accessPosition,
                deadline = deadline,
                isPrefixCompressed = isPrefixCompressed
              )
          }
      }
  }

  case class Group(private var _minKey: Slice[Byte],
                   private var _maxKey: MaxKey[Slice[Byte]],
                   segmentCache: CacheNOIO[(KeyOrder[Slice[Byte]], KeyValueLimiter, SegmentIO), SegmentCache],
                   nextIndexOffset: Int,
                   nextIndexSize: Int,
                   indexOffset: Int,
                   valueOffset: Int,
                   valueLength: Int,
                   accessPosition: Int,
                   deadline: Option[Deadline],
                   isPrefixCompressed: Boolean) extends Persistent with KeyValue.ReadOnly.Group {

    def isCached: Boolean =
      segmentCache.isCached

    override def indexEntryDeadline: Option[Deadline] = deadline

    override def key: Slice[Byte] =
      _minKey

    override def minKey: Slice[Byte] =
      _minKey

    override def maxKey: MaxKey[Slice[Byte]] =
      _maxKey

    override def unsliceKeys: Unit = {
      this._minKey = _minKey.unslice()
      this._maxKey = _maxKey.unslice()
    }

    def segment(implicit keyOrder: KeyOrder[Slice[Byte]],
                keyValueLimiter: KeyValueLimiter,
                config: SegmentIO): SegmentCache =
      segmentCache getOrElse {
        segmentCache.value(keyOrder, keyValueLimiter, config)
      }
  }
}
