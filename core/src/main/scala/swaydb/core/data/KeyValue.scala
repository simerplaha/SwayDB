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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.data

import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.{Failure, Success, Try}
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
import swaydb.data.repairAppendix.MaxKey
import swaydb.data.slice.{Reader, Slice}

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
      def toFromValue(): Try[Value.FromValue]

      def toRangeValue(): Try[Value.RangeValue]

      def time: Time
    }

    sealed trait Put extends KeyValue.ReadOnly.Fixed {
      def valueLength: Int

      def deadline: Option[Deadline]

      def hasTimeLeft(): Boolean

      def isOverdue(): Boolean =
        !hasTimeLeft()

      def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean

      def getOrFetchValue: Try[Option[Slice[Byte]]]

      def time: Time

      def toFromValue(): Try[Value.Put]

      def copyWithDeadlineAndTime(deadline: Option[Deadline], time: Time): KeyValue.ReadOnly.Put

      def copyWithTime(time: Time): KeyValue.ReadOnly.Put

    }

    sealed trait Remove extends KeyValue.ReadOnly.Fixed {
      def deadline: Option[Deadline]

      def hasTimeLeft(): Boolean

      def isOverdue(): Boolean =
        !hasTimeLeft()

      def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean

      def time: Time

      def toFromValue(): Try[Value.Remove]

      def toRemoveValue(): Value.Remove

      def copyWithTime(time: Time): KeyValue.ReadOnly.Remove
    }

    sealed trait Update extends KeyValue.ReadOnly.Fixed {
      def deadline: Option[Deadline]

      def hasTimeLeft(): Boolean

      def isOverdue(): Boolean =
        !hasTimeLeft()

      def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean

      def time: Time

      def getOrFetchValue: Try[Option[Slice[Byte]]]

      def toFromValue(): Try[Value.Update]

      def toPut(): KeyValue.ReadOnly.Put

      def toPut(deadline: Option[Deadline]): KeyValue.ReadOnly.Put

      def copyWithDeadlineAndTime(deadline: Option[Deadline], time: Time): KeyValue.ReadOnly.Update

      def copyWithDeadline(deadline: Option[Deadline]): KeyValue.ReadOnly.Update

      def copyWithTime(time: Time): KeyValue.ReadOnly.Update
    }

    sealed trait Function extends KeyValue.ReadOnly.Fixed {
      def time: Time

      def getOrFetchFunction: Try[Slice[Byte]]

      def toFromValue(): Try[Value.Function]

      def copyWithTime(time: Time): Function
    }

    sealed trait PendingApply extends KeyValue.ReadOnly.Fixed {
      def getOrFetchApplies: Try[Slice[Value.Apply]]

      def toFromValue(): Try[Value.PendingApply]

      def time: Time

      def deadline: Option[Deadline]
    }

    object Range {
      implicit class RangeImplicit(range: KeyValue.ReadOnly.Range) {
        def contains(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
          import keyOrder._
          key >= range.fromKey && key < range.toKey
        }
      }
    }

    sealed trait Range extends KeyValue.ReadOnly with SegmentResponse {
      def fromKey: Slice[Byte]

      def toKey: Slice[Byte]

      def fetchFromValue: Try[Option[Value.FromValue]]

      def fetchRangeValue: Try[Value.RangeValue]

      def fetchFromAndRangeValue: Try[(Option[Value.FromValue], Value.RangeValue)]

      def fetchFromOrElseRangeValue: Try[Value.FromValue] =
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
      }
    }

    sealed trait Group extends KeyValue.ReadOnly with CacheAble {
      def minKey: Slice[Byte]

      def maxKey: MaxKey[Slice[Byte]]

      def header(): Try[GroupHeader]

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
    //@formatter:off
    val isRemoveRange: Boolean
    val isRange: Boolean
    val isGroup: Boolean
    val hasRemove: Boolean
    val previous: Option[KeyValue.WriteOnly]
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
      if(!hasValueEntryBytes && currentEndValueOffsetPosition == 0)
        0
      else
        currentEndValueOffsetPosition + 1
    def value: Option[Slice[Byte]]
    //@formatter:on

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

      def isOverdue(): Boolean =
        !hasTimeLeft()

      def time: Time
    }

    sealed trait Range extends KeyValue.WriteOnly {
      val isRange: Boolean = true

      def fromKey: Slice[Byte]

      def toKey: Slice[Byte]

      def fromValue: Option[Value.FromValue]

      def rangeValue: Value.RangeValue

      def fetchFromValue: Try[Option[Value.FromValue]]

      def fetchRangeValue: Try[Value.RangeValue]

      def fetchFromAndRangeValue: Try[(Option[Value.FromValue], Value.RangeValue)]
    }

    sealed trait Group extends KeyValue.WriteOnly {
      val isRange: Boolean = true

      def minKey: Slice[Byte]

      def maxKey: MaxKey[Slice[Byte]]

      def fullKey: Slice[Byte]

      def keyValues: Iterable[KeyValue.WriteOnly]

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

    override def getOrFetchValue: Try[Option[Slice[Byte]]] =
      Success(value)

    override def toFromValue(): Try[Value.Put] =
      Success(Value.Put(value, deadline, time))

    override def copyWithDeadlineAndTime(deadline: Option[Deadline],
                                         time: Time): Put =
      copy(deadline = deadline, time = time)

    override def copyWithTime(time: Time): Put =
      copy(time = time)

    //ahh not very type-safe.
    override def toRangeValue(): Try[Value.RangeValue] =
      Failure(new Exception("Put cannot be converted to RangeValue"))
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

    override def getOrFetchValue: Try[Option[Slice[Byte]]] =
      Success(value)

    override def toFromValue(): Try[Value.Update] =
      Success(Value.Update(value, deadline, time))

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

    override def toRangeValue(): Try[Value.Update] =
      toFromValue()
  }

  case class Function(key: Slice[Byte],
                      function: Slice[Byte],
                      time: Time) extends KeyValue.ReadOnly.Function with Memory.Fixed {

    override def indexEntryDeadline: Option[Deadline] = None

    override def getOrFetchFunction: Try[Slice[Byte]] =
      Success(function)

    override def toFromValue(): Try[Value.Function] =
      Success(Value.Function(function, time))

    override def copyWithTime(time: Time): Function =
      copy(time = time)

    override def toRangeValue(): Try[Value.Function] =
      toFromValue()
  }

  case class PendingApply(key: Slice[Byte],
                          applies: Slice[Value.Apply]) extends KeyValue.ReadOnly.PendingApply with Memory.Fixed {

    override val deadline =
      Segment.getNearestDeadline(None, applies)

    override def indexEntryDeadline: Option[Deadline] = deadline

    def time = Time.fromApplies(applies)

    override def getOrFetchApplies: Try[Slice[Value.Apply]] =
      Success(applies)

    override def toFromValue(): Try[Value.PendingApply] =
      Success(Value.PendingApply(applies))

    override def toRangeValue(): Try[Value.PendingApply] =
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

    override def toFromValue(): Try[Value.Remove] =
      Success(toRemoveValue())

    override def toRangeValue(): Try[Value.Remove] =
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

    override def fetchFromValue: Try[Option[Value.FromValue]] =
      Success(fromValue)

    override def fetchRangeValue: Try[Value.RangeValue] =
      Success(rangeValue)

    override def fetchFromAndRangeValue: Try[(Option[Value.FromValue], Value.RangeValue)] =
      Success(fromValue, rangeValue)

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
        getFooter = groupDecompressor.footer,
        createReader = groupDecompressor.reader
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

  implicit class TransientImplicits(transient: Transient)(implicit keyOrder: KeyOrder[Slice[Byte]]) {

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
          Memory.Range(range.fromKey, range.toKey, range.fromValue, range.rangeValue)

        case _: Transient.Group =>
          //todo make this type-safe instead.
          throw new Exception("toMemoryResponse invoked on a Group")
      }
  }

  case class Remove(key: Slice[Byte],
                    deadline: Option[Deadline],
                    time: Time,
                    previous: Option[KeyValue.WriteOnly],
                    falsePositiveRate: Double) extends Transient with KeyValue.WriteOnly.Fixed {
    override val hasRemove: Boolean = true
    override val isRange: Boolean = false
    override val isGroup: Boolean = false
    override val isRemoveRange = false
    override val value: Option[Slice[Byte]] = None
    override val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      EntryWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = false
      )

    override def fullKey = key

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)
    override val stats =
      Stats(
        key = indexEntryBytes,
        value = None,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRange,
        isRange = isRange,
        isGroup = isGroup,
        bloomFiltersItemCount = 1,
        isPut = false,
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
                 compressDuplicateValues: Boolean) extends Transient with KeyValue.WriteOnly.Fixed {

    override val hasRemove: Boolean = previous.exists(_.hasRemove)
    override val isRemoveRange = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      EntryWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = compressDuplicateValues
      )

    override val hasValueEntryBytes: Boolean =
      previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        key = indexEntryBytes,
        value = valueEntryBytes,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRange,
        isRange = isRange,
        isGroup = isGroup,
        bloomFiltersItemCount = 1,
        isPut = true,
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
                    compressDuplicateValues: Boolean) extends Transient with KeyValue.WriteOnly.Fixed {
    override val hasRemove: Boolean = previous.exists(_.hasRemove)
    override val isRemoveRange = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false
    override def fullKey = key

    override def updateStats(falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Transient.Update =
      this.copy(falsePositiveRate = falsePositiveRate, previous = previous)

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      EntryWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = compressDuplicateValues
      )
    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        key = indexEntryBytes,
        value = valueEntryBytes,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRange,
        isRange = isRange,
        isGroup = isGroup,
        bloomFiltersItemCount = 1,
        isPut = false,
        previous = previous,
        deadline = deadline
      )



  }

  case class Function(key: Slice[Byte],
                      function: Slice[Byte],
                      value: Option[Slice[Byte]],
                      deadline: Option[Deadline],
                      time: Time,
                      previous: Option[KeyValue.WriteOnly],
                      falsePositiveRate: Double,
                      compressDuplicateValues: Boolean) extends Transient with KeyValue.WriteOnly.Fixed {
    override val hasRemove: Boolean = previous.exists(_.hasRemove)
    override val isRemoveRange = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false

    override def fullKey = key

    override def updateStats(falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Transient.Function =
      this.copy(falsePositiveRate = falsePositiveRate, previous = previous)

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      EntryWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = compressDuplicateValues
      )
    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        key = indexEntryBytes,
        value = valueEntryBytes,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRange,
        isRange = isRange,
        isGroup = isGroup,
        bloomFiltersItemCount = 1,
        isPut = false,
        previous = previous,
        deadline = deadline
      )

  }

  object PendingApply {
    def apply(key: Slice[Byte],
              applies: Slice[Value.Apply],
              previous: Option[KeyValue.WriteOnly],
              falsePositiveRate: Double,
              compressDuplicateValues: Boolean): PendingApply = {
      val bytesRequired = ValueSerializer.bytesRequired(applies)
      val bytes = Slice.create[Byte](bytesRequired)
      ValueSerializer.write(applies)(bytes)

      new PendingApply(
        key = key,
        applies = applies,
        value = Some(bytes),
        previous = previous,
        falsePositiveRate = falsePositiveRate,
        compressDuplicateValues = compressDuplicateValues
      )
    }
  }

  case class PendingApply(key: Slice[Byte],
                          applies: Slice[Value.Apply],
                          value: Option[Slice[Byte]],
                          previous: Option[KeyValue.WriteOnly],
                          falsePositiveRate: Double,
                          compressDuplicateValues: Boolean) extends Transient with KeyValue.WriteOnly.Fixed {
    override val hasRemove: Boolean = previous.exists(_.hasRemove)
    override val isRemoveRange = false
    override val isGroup: Boolean = false
    override val isRange: Boolean = false
    override val deadline: Option[Deadline] =
      Segment.getNearestDeadline(None, applies)

    def time = Time.fromApplies(applies)

    override def fullKey = key

    override def updateStats(falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Transient.PendingApply =
      this.copy(falsePositiveRate = falsePositiveRate, previous = previous)

    override def hasTimeLeft(): Boolean =
      true

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      EntryWriter.write(
        current = this,
        currentTime = time,
        compressDuplicateValues = compressDuplicateValues
      )
    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        key = indexEntryBytes,
        value = valueEntryBytes,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRange,
        isRange = isRange,
        isGroup = isGroup,
        bloomFiltersItemCount = 1,
        isPut = false,
        previous = previous,
        deadline = deadline
      )


  }

  object Range {

    def apply[R <: Value.RangeValue](fromKey: Slice[Byte],
                                     toKey: Slice[Byte],
                                     rangeValue: R,
                                     falsePositiveRate: Double,
                                     previous: Option[KeyValue.WriteOnly])(implicit rangeValueSerializer: RangeValueSerializer[Unit, R]): Range = {
      val bytesRequired = rangeValueSerializer.bytesRequired((), rangeValue)
      val value = if (bytesRequired == 0) None else Some(Slice.create[Byte](bytesRequired))
      value.foreach(rangeValueSerializer.write((), rangeValue, _))
      val fullKey = Bytes.compressJoin(fromKey, toKey)
      new Range(
        fullKey = fullKey,
        fromKey = fromKey,
        toKey = toKey,
        fromValue = None,
        rangeValue = rangeValue,
        value = value,
        previous = previous,
        falsePositiveRate = falsePositiveRate
      )
    }

    def apply[F <: Value.FromValue, R <: Value.RangeValue](fromKey: Slice[Byte],
                                                           toKey: Slice[Byte],
                                                           fromValue: Option[F],
                                                           rangeValue: R,
                                                           falsePositiveRate: Double,
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
        falsePositiveRate = falsePositiveRate
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
                   falsePositiveRate: Double) extends Transient with KeyValue.WriteOnly.Range {

    def key = fromKey

    override val hasRemove: Boolean = previous.exists(_.hasRemove)
    override val isRemoveRange = rangeValue.isRemove
    override val isGroup: Boolean = false
    override val deadline: Option[Deadline] = None
    override def updateStats(falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Transient.Range =
      this.copy(falsePositiveRate = falsePositiveRate, previous = previous)

    override def fetchFromValue: Try[Option[Value.FromValue]] =
      Success(fromValue)

    override def fetchRangeValue: Try[Value.RangeValue] =
      Success(rangeValue)

    override def fetchFromAndRangeValue: Try[(Option[Value.FromValue], Value.RangeValue)] =
      Success(fromValue, rangeValue)

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      EntryWriter.write(
        current = this,
        currentTime = Time.empty,
        //It's highly likely that two sequential key-values within the same range have the same value after the range split occurs. So this is always set to true.
        compressDuplicateValues = true
      )

    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        key = indexEntryBytes,
        value = valueEntryBytes,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRange,
        isRange = isRange,
        isGroup = isGroup,
        previous = previous,
        bloomFiltersItemCount = 1,
        isPut = fromValue.exists(_.isInstanceOf[Value.Put]),
        deadline = None
      )


  }

  object Group {

    def apply(keyValues: Iterable[KeyValue.WriteOnly],
              indexCompression: CompressionInternal,
              valueCompression: CompressionInternal,
              falsePositiveRate: Double,
              previous: Option[KeyValue.WriteOnly]): Try[Option[Transient.Group]] =
      GroupCompressor.compress(
        keyValues = keyValues,
        indexCompressions = Seq(indexCompression),
        valueCompressions = Seq(valueCompression),
        falsePositiveRate = falsePositiveRate,
        previous = previous
      )

    def apply(keyValues: Iterable[KeyValue.WriteOnly],
              indexCompressions: Seq[CompressionInternal],
              valueCompressions: Seq[CompressionInternal],
              falsePositiveRate: Double,
              previous: Option[KeyValue.WriteOnly]): Try[Option[Transient.Group]] =
      GroupCompressor.compress(
        keyValues = keyValues,
        indexCompressions = indexCompressions,
        valueCompressions = valueCompressions,
        falsePositiveRate = falsePositiveRate,
        previous = previous
      )
  }

  case class Group(minKey: Slice[Byte],
                   maxKey: MaxKey[Slice[Byte]],
                   fullKey: Slice[Byte],
                   compressedKeyValues: Slice[Byte],
                   //the deadline is the nearest deadline in the Group's key-values.
                   deadline: Option[Deadline],
                   keyValues: Iterable[KeyValue.WriteOnly],
                   previous: Option[KeyValue.WriteOnly],
                   falsePositiveRate: Double) extends Transient with KeyValue.WriteOnly.Group {

    override def key = minKey

    override val hasRemove: Boolean = previous.exists(_.hasRemove)
    override val isRemoveRange: Boolean = keyValues.last.stats.hasRemoveRange
    override val isRange: Boolean = keyValues.last.stats.hasRange
    override val isGroup: Boolean = true
    override val value: Option[Slice[Byte]] = Some(compressedKeyValues)

    override def updateStats(falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Transient.Group =
      this.copy(falsePositiveRate = falsePositiveRate, previous = previous)

    val (indexEntryBytes, valueEntryBytes, currentStartValueOffsetPosition, currentEndValueOffsetPosition) =
      EntryWriter.write(
        current = this,
        currentTime = Time.empty,
        //it's highly unlikely that 2 groups after compression will have duplicate values.
        //compressDuplicateValues check is unnecessary since the value bytes of a group can be large.
        compressDuplicateValues = false
      )
    override val hasValueEntryBytes: Boolean = previous.exists(_.hasValueEntryBytes) || valueEntryBytes.exists(_.nonEmpty)

    val stats =
      Stats(
        key = indexEntryBytes,
        value = valueEntryBytes,
        falsePositiveRate = falsePositiveRate,
        isRemoveRange = isRemoveRange,
        isRange = isRange,
        isGroup = isGroup,
        previous = previous,
        bloomFiltersItemCount = keyValues.last.stats.bloomFilterItemsCount,
        isPut = keyValues.last.stats.hasPut,
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

  /**
    * This function is NOT thread-safe and is mutable. It should always be invoke at the time of creation
    * and before inserting into the Segment's cache.
    */
  def unsliceIndexBytes: Unit
}

private[core] object Persistent {

  sealed trait SegmentResponse extends KeyValue.ReadOnly.SegmentResponse with Persistent {
    def toMemory(): Try[Memory.SegmentResponse]

    def toMemoryResponseOption(): Try[Option[Memory.SegmentResponse]] =
      toMemory() map (Some(_))

  }
  sealed trait Fixed extends Persistent.SegmentResponse with KeyValue.ReadOnly.Fixed

  object Remove {
    def apply(key: Slice[Byte],
              deadline: Option[Deadline],
              time: Time): Persistent.Remove =
      Persistent.Remove(
        _key = key,
        deadline = deadline,
        _time = time,
        indexOffset = 0,
        nextIndexOffset = -1,
        nextIndexSize = 0
      )
  }

  case class Remove(private var _key: Slice[Byte],
                    deadline: Option[Deadline],
                    private var _time: Time,
                    indexOffset: Int,
                    nextIndexOffset: Int,
                    nextIndexSize: Int) extends Persistent.Fixed with KeyValue.ReadOnly.Remove {
    override val valueLength: Int = 0
    override val isValueDefined: Boolean = true

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

    override val valueOffset: Int = 0

    override def toMemory(): Try[Memory.Remove] =
      Success {
        Memory.Remove(
          key = key,
          deadline = deadline,
          time = time
        )
      }

    override def copyWithTime(time: Time): ReadOnly.Remove =
      copy(_time = time)

    override def toFromValue(): Try[Value.Remove] =
      Success(toRemoveValue())

    override def toRangeValue(): Try[Value.Remove] =
      toFromValue()

    override def toRemoveValue(): Value.Remove =
      Value.Remove(deadline, time)
  }

  object Put {
    def apply(key: Slice[Byte],
              deadline: Option[Deadline],
              time: Time,
              value: Option[Slice[Byte]]): Persistent.Put =
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
        valueLength = value.map(_.size).getOrElse(0)
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
                 valueLength: Int) extends Persistent.Fixed with KeyValue.ReadOnly.Put {
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

    override def getOrFetchValue: Try[Option[Slice[Byte]]] =
      lazyValueReader.getOrFetchValue

    override def isValueDefined: Boolean =
      lazyValueReader.isValueDefined

    override def toFromValue(): Try[Value.Put] =
      getOrFetchValue map {
        value =>
          Value.Put(value, deadline, time)
      }

    override def toRangeValue(): Try[Value.RangeValue] =
      Failure(new Exception("Put cannot be converted to RangeValue"))

    override def toMemory(): Try[Memory.Put] =
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
                    valueLength: Int) extends Persistent.Fixed with KeyValue.ReadOnly.Update {
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

    def getOrFetchValue: Try[Option[Slice[Byte]]] =
      lazyValueReader.getOrFetchValue

    override def toFromValue(): Try[Value.Update] =
      getOrFetchValue map {
        value =>
          Value.Update(value, deadline, time)
      }

    override def toRangeValue(): Try[Value.Update] =
      toFromValue()

    override def toMemory(): Try[Memory.Update] =
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
        valueLength = valueLength
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
        valueLength = valueLength
      )

  }

  case class Function(private var _key: Slice[Byte],
                      private val lazyFunctionReader: LazyFunctionReader,
                      private var _time: Time,
                      nextIndexOffset: Int,
                      nextIndexSize: Int,
                      indexOffset: Int,
                      valueOffset: Int,
                      valueLength: Int) extends Persistent.Fixed with KeyValue.ReadOnly.Function {
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

    def getOrFetchFunction: Try[Slice[Byte]] =
      lazyFunctionReader.getOrFetchFunction

    override def toFromValue(): Try[Value.Function] =
      getOrFetchFunction map {
        value =>
          Value.Function(value, time)
      }

    override def toRangeValue(): Try[Value.Function] =
      toFromValue()

    override def toMemory(): Try[Memory.Function] =
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
              time: Time): Persistent.PendingApply =
      Persistent.PendingApply(
        _key = key,
        _time = time,
        deadline = deadline,
        lazyValueReader = ActivePendingApplyValueReader(applies),
        nextIndexOffset = -1,
        nextIndexSize = 0,
        indexOffset = 0,
        valueOffset = 0,
        valueLength = 0
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
                          valueLength: Int) extends Persistent.Fixed with KeyValue.ReadOnly.PendingApply {
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

    override def getOrFetchApplies: Try[Slice[Value.Apply]] =
      lazyValueReader.getOrFetchApplies

    override def toFromValue(): Try[Value.PendingApply] =
      lazyValueReader.getOrFetchApplies map {
        applies =>
          Value.PendingApply(applies)
      }

    override def toRangeValue(): Try[Value.PendingApply] =
      toFromValue()

    override def toMemory(): Try[Memory.PendingApply] =
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
              valueLength: Int): Try[Persistent.Range] =
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
            valueLength = valueLength
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
                   valueLength: Int) extends Persistent.SegmentResponse with KeyValue.ReadOnly.Range {

    def fromKey = _fromKey

    def toKey = _toKey

    override def indexEntryDeadline: Option[Deadline] = None

    override def unsliceIndexBytes: Unit = {
      this._fromKey = _fromKey.unslice()
      this._toKey = _toKey.unslice()
    }

    override def key: Slice[Byte] =
      _fromKey

    override def fetchFromValue: Try[Option[Value.FromValue]] =
      lazyRangeValueReader.fetchFromValue

    override def fetchRangeValue: Try[Value.RangeValue] =
      lazyRangeValueReader.fetchRangeValue

    override def fetchFromAndRangeValue: Try[(Option[Value.FromValue], Value.RangeValue)] =
      lazyRangeValueReader.fetchFromAndRangeValue

    override def isValueDefined: Boolean =
      lazyRangeValueReader.isValueDefined

    override def toMemory(): Try[Memory.Range] =
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
              deadline: Option[Deadline]): Try[Group] =
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
            deadline = deadline
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
                   deadline: Option[Deadline]) extends Persistent with KeyValue.ReadOnly.Group {

    lazy val segmentCacheInitializer =
      new SegmentCacheInitializer(
        id = "Persistent.Group",
        minKey = minKey,
        maxKey = maxKey,
        //persistent key-value's key do not have be sliced either because the decompressed bytes are still in memory.
        //slicing will just use more memory. On memory overflow the Group itself will get dropped and hence all the
        //key-values inside the group's SegmentCache will also be GC'd.
        unsliceKey = false,
        getFooter = groupDecompressor.footer,
        createReader = groupDecompressor.reader
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
