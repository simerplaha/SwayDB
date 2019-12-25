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

import swaydb.IO
import swaydb.core.cache.{Cache, CacheNoIO}
import swaydb.core.map.serializer.RangeValueSerializer.OptionRangeValueSerializer
import swaydb.core.map.serializer.{RangeValueSerializer, ValueSerializer}
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.Bytes
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOptional}
import swaydb.data.util.Maybe.Maybe
import swaydb.data.util.{SomeOrNone, SomeOrNoneCovariant}

import scala.concurrent.duration.{Deadline, FiniteDuration}

private[core] sealed trait KeyValueOptional {
  def getUnsafe: KeyValue

  def toOptional: Option[KeyValue] =
    this match {
      case optional: MemoryOptional =>
        optional.toOptionS

      case optional: PersistentOptional =>
        optional.toOptionS
    }
}

private[core] sealed trait KeyValue {
  def key: Slice[Byte]

  def indexEntryDeadline: Option[Deadline]
  def toMemory: Memory

  def keyLength =
    key.size
}

private[core] object KeyValue {

  sealed trait Null extends KeyValueOptional

  /**
   * Key-values that can be added to [[swaydb.core.actor.MemorySweeper]].
   *
   * These key-values can remain in memory depending on the cacheSize and are dropped or uncompressed on overflow.
   */
  sealed trait CacheAble extends KeyValue {
    def valueLength: Int
  }

  /**
   * An API response type expected from a [[swaydb.core.map.Map]] or [[swaydb.core.segment.Segment]].
   */
  sealed trait Fixed extends KeyValue {
    def toFromValue(): Value.FromValue

    def toRangeValue(): Value.RangeValue

    def time: Time
  }

  sealed trait Put extends KeyValue.Fixed {
    def valueLength: Int
    def deadline: Option[Deadline]
    def hasTimeLeft(): Boolean
    def isOverdue(): Boolean = !hasTimeLeft()
    def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean
    def getOrFetchValue: SliceOptional[Byte]
    def time: Time
    def toFromValue(): Value.Put
    def copyWithDeadlineAndTime(deadline: Option[Deadline], time: Time): KeyValue.Put
    def copyWithTime(time: Time): KeyValue.Put
  }

  sealed trait Remove extends KeyValue.Fixed {
    def deadline: Option[Deadline]
    def hasTimeLeft(): Boolean
    def isOverdue(): Boolean = !hasTimeLeft()
    def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean
    def time: Time
    def toFromValue(): Value.Remove
    def toRemoveValue(): Value.Remove
    def copyWithTime(time: Time): KeyValue.Remove
  }

  sealed trait Update extends KeyValue.Fixed {
    def deadline: Option[Deadline]
    def hasTimeLeft(): Boolean
    def isOverdue(): Boolean = !hasTimeLeft()
    def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean
    def time: Time
    def getOrFetchValue: SliceOptional[Byte]
    def toFromValue(): Value.Update
    def toPut(): KeyValue.Put
    def toPut(deadline: Option[Deadline]): KeyValue.Put
    def copyWithDeadlineAndTime(deadline: Option[Deadline], time: Time): KeyValue.Update
    def copyWithDeadline(deadline: Option[Deadline]): KeyValue.Update
    def copyWithTime(time: Time): KeyValue.Update
  }

  sealed trait Function extends KeyValue.Fixed {
    def time: Time
    def getOrFetchFunction: Slice[Byte]
    def toFromValue(): Value.Function
    def copyWithTime(time: Time): Function
  }

  sealed trait PendingApply extends KeyValue.Fixed {
    def getOrFetchApplies: Slice[Value.Apply]
    def toFromValue(): Value.PendingApply
    def time: Time
    def deadline: Option[Deadline]
  }

  object Range {
    implicit class RangeImplicit(range: KeyValue.Range) {
      @inline def contains(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
        import keyOrder._
        key >= range.fromKey && key < range.toKey
      }

      @inline def containsLower(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
        import keyOrder._
        key > range.fromKey && key <= range.toKey
      }
    }
  }

  sealed trait Range extends KeyValue {
    def fromKey: Slice[Byte]
    def toKey: Slice[Byte]
    def fetchFromValueUnsafe: Value.FromValueOption
    def fetchRangeValueUnsafe: Value.RangeValue
    def fetchFromAndRangeValueUnsafe: (Value.FromValueOption, Value.RangeValue)
    def fetchFromOrElseRangeValueUnsafe: Value.FromValue = {
      val (fromValue, rangeValue) = fetchFromAndRangeValueUnsafe
      fromValue getOrElseS rangeValue
    }
  }

}

private[swaydb] sealed trait MemoryOptional extends SomeOrNone[MemoryOptional, Memory] with KeyValueOptional {
  override def noneS: MemoryOptional = Memory.Null

  override def self = this

  override def getUnsafe: KeyValue =
    getS
}

private[swaydb] sealed trait Memory extends KeyValue with MemoryOptional {
  def id: Byte
  def isRange: Boolean
  def isRemoveRangeMayBe: Boolean
  def isPut: Boolean
  def persistentTime: Time
  def mergedKey: Slice[Byte]
  def value: SliceOptional[Byte]
  def deadline: Option[Deadline]

  override def isNoneS: Boolean =
    false

  override def getS: Memory =
    this
}

private[swaydb] object Memory {

  final object Null extends MemoryOptional with KeyValue.Null {
    override def isNoneS: Boolean =
      true

    override def getS: Memory =
      throw new Exception("Is Null")

  }

  implicit class MemoryIterableImplicits(keyValues: Slice[Memory]) {
    @inline def maxKey(): MaxKey[Slice[Byte]] =
      keyValues.last match {
        case range: Memory.Range =>
          MaxKey.Range(range.fromKey, range.toKey)

        case fixed: Memory.Fixed =>
          MaxKey.Fixed(fixed.key)
      }

    @inline def minKey: Slice[Byte] =
      keyValues.head.key
  }

  sealed trait Fixed extends Memory with KeyValue.Fixed {
    def isRange: Boolean = false
  }

  object Put {
    final val id = 1.toByte
  }

  //if value is empty byte slice, return None instead of empty Slice.We do not store empty byte arrays.
  def compressibleValue(keyValue: Memory): SliceOptional[Byte] =
    if (keyValue.value.existsC(_.isEmpty))
      Slice.Null
    else
      keyValue.value

  def hasSameValue(left: Memory, right: Memory): Boolean =
    (left, right) match {
      //Remove
      case (left: Memory.Remove, right: Memory.Remove) => true
      case (left: Memory.Remove, right: Memory.Put) => right.value.isNoneC
      case (left: Memory.Remove, right: Memory.Update) => right.value.isNoneC
      case (left: Memory.Remove, right: Memory.Function) => false
      case (left: Memory.Remove, right: Memory.PendingApply) => false
      case (left: Memory.Remove, right: Memory.Range) => false
      //Put
      case (left: Memory.Put, right: Memory.Remove) => left.value.isNoneC
      case (left: Memory.Put, right: Memory.Put) => left.value == right.value
      case (left: Memory.Put, right: Memory.Update) => left.value == right.value
      case (left: Memory.Put, right: Memory.Function) => left.value containsC right.function
      case (left: Memory.Put, right: Memory.PendingApply) => false
      case (left: Memory.Put, right: Memory.Range) => false
      //Update
      case (left: Memory.Update, right: Memory.Remove) => left.value.isNoneC
      case (left: Memory.Update, right: Memory.Put) => left.value == right.value
      case (left: Memory.Update, right: Memory.Update) => left.value == right.value
      case (left: Memory.Update, right: Memory.Function) => left.value containsC right.function
      case (left: Memory.Update, right: Memory.PendingApply) => false
      case (left: Memory.Update, right: Memory.Range) => false
      //Function
      case (left: Memory.Function, right: Memory.Remove) => false
      case (left: Memory.Function, right: Memory.Put) => right.value containsC left.function
      case (left: Memory.Function, right: Memory.Update) => right.value containsC left.function
      case (left: Memory.Function, right: Memory.Function) => left.function == right.function
      case (left: Memory.Function, right: Memory.PendingApply) => false
      case (left: Memory.Function, right: Memory.Range) => false
      //PendingApply
      case (left: Memory.PendingApply, right: Memory.Remove) => false
      case (left: Memory.PendingApply, right: Memory.Put) => false
      case (left: Memory.PendingApply, right: Memory.Update) => false
      case (left: Memory.PendingApply, right: Memory.Function) => false
      case (left: Memory.PendingApply, right: Memory.PendingApply) => left.applies == right.applies
      case (left: Memory.PendingApply, right: Memory.Range) => false
      //Range
      case (left: Memory.Range, right: Memory.Remove) => false
      case (left: Memory.Range, right: Memory.Put) => false
      case (left: Memory.Range, right: Memory.Update) => false
      case (left: Memory.Range, right: Memory.Function) => false
      case (left: Memory.Range, right: Memory.PendingApply) => false
      case (left: Memory.Range, right: Memory.Range) => left.fromValue == right.fromValue && left.rangeValue == right.rangeValue
    }

  case class Put(key: Slice[Byte],
                 value: SliceOptional[Byte],
                 deadline: Option[Deadline],
                 time: Time) extends Memory.Fixed with KeyValue.Put {

    override def id: Byte = Put.id

    override def isPut: Boolean = true

    override def persistentTime: Time = time

    override def mergedKey = key

    override def isRemoveRangeMayBe = false

    override def indexEntryDeadline: Option[Deadline] = deadline

    override def valueLength: Int =
      value.mapC(_.size).getOrElse(0)

    def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean =
      deadline.forall(deadline => (deadline - minus).hasTimeLeft())

    override def getOrFetchValue: SliceOptional[Byte] =
      value

    override def toFromValue(): Value.Put =
      Value.Put(value, deadline, time)

    override def copyWithDeadlineAndTime(deadline: Option[Deadline],
                                         time: Time): Put =
      copy(deadline = deadline, time = time)

    override def copyWithTime(time: Time): Put =
      copy(time = time)

    //to do - make type-safe.
    override def toRangeValue(): Value.RangeValue =
      throw IO.throwable("Put cannot be converted to RangeValue")

    override def toMemory: Memory.Put =
      this
  }

  object Update {
    final val id = 2.toByte
  }

  case class Update(key: Slice[Byte],
                    value: SliceOptional[Byte],
                    deadline: Option[Deadline],
                    time: Time) extends KeyValue.Update with Memory.Fixed {

    override def id: Byte = Update.id

    override def isPut: Boolean = false

    override def persistentTime: Time = time

    override def isRemoveRangeMayBe = false

    override def mergedKey = key

    override def indexEntryDeadline: Option[Deadline] = deadline

    def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean =
      deadline.forall(deadline => (deadline - minus).hasTimeLeft())

    override def getOrFetchValue: SliceOptional[Byte] =
      value

    override def toFromValue(): Value.Update =
      Value.Update(value, deadline, time)

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

    override def toRangeValue(): Value.Update =
      toFromValue()

    override def toMemory: Memory.Update =
      this
  }

  object Function {
    final val id = 3.toByte
  }

  case class Function(key: Slice[Byte],
                      function: Slice[Byte],
                      time: Time) extends KeyValue.Function with Memory.Fixed {

    override def id: Byte = Function.id

    override def isPut: Boolean = false

    override def persistentTime: Time = time

    override def mergedKey = key

    override def isRemoveRangeMayBe = false

    override def indexEntryDeadline: Option[Deadline] = None

    override def value: SliceOptional[Byte] = function

    override def deadline: Option[Deadline] = None

    override def getOrFetchFunction: Slice[Byte] =
      function

    override def toFromValue(): Value.Function =
      Value.Function(function, time)

    override def copyWithTime(time: Time): Function =
      copy(time = time)

    override def toRangeValue(): Value.Function =
      toFromValue()

    override def toMemory: Memory.Function =
      this
  }

  object PendingApply {
    final val id = 4.toByte
  }

  case class PendingApply(key: Slice[Byte],
                          applies: Slice[Value.Apply]) extends KeyValue.PendingApply with Memory.Fixed {

    override def id: Byte = PendingApply.id

    override def persistentTime: Time = time

    override def isPut: Boolean = false

    override def mergedKey = key

    override def isRemoveRangeMayBe = false

    override lazy val value: SliceOptional[Byte] = ValueSerializer.writeBytes(applies)

    override val deadline =
      Segment.getNearestDeadline(None, applies)

    override def indexEntryDeadline: Option[Deadline] = deadline

    def time = Time.fromApplies(applies)

    override def getOrFetchApplies: Slice[Value.Apply] =
      applies

    override def toFromValue(): Value.PendingApply =
      Value.PendingApply(applies)

    override def toRangeValue(): Value.PendingApply =
      toFromValue()

    override def toMemory: Memory.PendingApply =
      this
  }

  object Remove {
    final val id = 5.toByte
  }

  case class Remove(key: Slice[Byte],
                    deadline: Option[Deadline],
                    time: Time) extends Memory.Fixed with KeyValue.Remove {

    override def id: Byte = Remove.id

    override def isPut: Boolean = false

    override def persistentTime: Time = time

    override def mergedKey = key

    override def isRemoveRangeMayBe = false

    override def indexEntryDeadline: Option[Deadline] = deadline

    override def value: SliceOptional[Byte] = Slice.Null

    def hasTimeLeft(): Boolean =
      deadline.exists(_.hasTimeLeft())

    def hasTimeLeftAtLeast(atLeast: FiniteDuration): Boolean =
      deadline.exists(deadline => (deadline - atLeast).hasTimeLeft())

    def toRemoveValue(): Value.Remove =
      Value.Remove(deadline, time)

    override def copyWithTime(time: Time): Remove =
      copy(time = time)

    override def toFromValue(): Value.Remove =
      toRemoveValue()

    override def toRangeValue(): Value.Remove =
      toFromValue()

    override def toMemory: Memory.Remove =
      this
  }

  object Range {

    final val id = 6.toByte

    def apply(fromKey: Slice[Byte],
              toKey: Slice[Byte],
              fromValue: Value.FromValue,
              rangeValue: Value.RangeValue): Range =
      new Range(
        fromKey = fromKey,
        toKey = toKey,
        fromValue = fromValue,
        rangeValue = rangeValue
      )
  }

  case class Range(fromKey: Slice[Byte],
                   toKey: Slice[Byte],
                   fromValue: Value.FromValueOption,
                   rangeValue: Value.RangeValue) extends Memory with KeyValue.Range {

    override def persistentTime: Time = Time.empty

    override def isRemoveRangeMayBe: Boolean = rangeValue.hasRemoveMayBe

    override def id: Byte = Range.id

    override def isPut: Boolean = false

    def isRange: Boolean = true

    override lazy val mergedKey: Slice[Byte] = Bytes.compressJoin(fromKey, toKey)

    override lazy val value: SliceOptional[Byte] = {
      val bytesRequired = OptionRangeValueSerializer.bytesRequired(fromValue, rangeValue)
      val bytes = if (bytesRequired == 0) Slice.Null else Slice.create[Byte](bytesRequired)
      bytes.foreachC(OptionRangeValueSerializer.write(fromValue, rangeValue, _))
      bytes
    }

    override def deadline: Option[Deadline] = None

    override def key: Slice[Byte] = fromKey

    override def indexEntryDeadline: Option[Deadline] = None

    override def fetchFromValueUnsafe: Value.FromValueOption =
      fromValue

    override def fetchRangeValueUnsafe: Value.RangeValue =
      rangeValue

    override def fetchFromAndRangeValueUnsafe: (Value.FromValueOption, Value.RangeValue) =
      (fromValue, rangeValue)

    override def toMemory: Memory.Range =
      this
  }
}

private[core] sealed trait PersistentOptional extends SomeOrNone[PersistentOptional, Persistent] with KeyValueOptional {
  override def noneS: PersistentOptional = Persistent.Null

  override def self = this

  def asPartial: Persistent.PartialOptional =
    if (this.isSomeS)
      getS
    else
      Persistent.Partial.Null
}

private[core] sealed trait Persistent extends KeyValue.CacheAble with Persistent.Partial with PersistentOptional {

  def indexOffset: Int
  val nextIndexOffset: Int
  val nextKeySize: Int
  val sortedIndexAccessPosition: Int

  override def isPartial: Boolean =
    false

  def valueLength: Int

  def valueOffset: Int

  def toMemory(): Memory

  def isValueCached: Boolean

  def toMemoryOption(): MemoryOptional =
    toMemory()

  override def isNoneS: Boolean =
    false

  override def getS: Persistent =
    this

  override def getUnsafe: Persistent =
    getS

  /**
   * This function is NOT thread-safe and is mutable. It should always be invoke at the time of creation
   * and before inserting into the Segment's cache.
   */
  def unsliceKeys: Unit
}

private[core] object Persistent {

  final object Null extends PersistentOptional with KeyValue.Null {
    override def isNoneS: Boolean = true
    override def getS: Persistent = throw new Exception("get on Persistent key-value that is none")
    override def getUnsafe: KeyValue = getS
  }

  /**
   * [[Partial]] key-values types are used in persistent databases
   * for quick search where the entire key-value parse is skipped
   * and only key is read for processing.
   */

  private[core] sealed trait PartialOptional extends SomeOrNoneCovariant[PartialOptional, Partial] {
    override def noneC: PartialOptional = Partial.Null

    def toPersistentOptional: PersistentOptional =
      if (isNoneC)
        Persistent.Null
      else
        getC.toPersistent
  }

  sealed trait Partial extends PartialOptional {
    def key: Slice[Byte]
    def indexOffset: Int
    def toPersistent: Persistent
    def isPartial: Boolean = true
    def get: Partial = this
  }

  object Partial {

    val noneMaybe: Maybe[Persistent.Partial] =
      null.asInstanceOf[Maybe[Persistent.Partial]]

    final object Null extends PartialOptional {
      override def getC: Partial = throw new Exception("Partial is of type Null")
      override def isNoneC: Boolean = true
    }

    trait Fixed extends Persistent.Partial {
      override def getC: Partial = this
      override def isNoneC: Boolean = false
    }

    trait Range extends Persistent.Partial {
      override def getC: Partial = this
      override def isNoneC: Boolean = false

      def fromKey: Slice[Byte]
      def toKey: Slice[Byte]
    }
  }

  sealed trait Fixed extends Persistent with KeyValue.Fixed with Partial.Fixed

  sealed trait Reader[T <: Persistent] {
    def apply(key: Slice[Byte],
              deadline: Option[Deadline],
              valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
              time: Time,
              nextIndexOffset: Int,
              nextKeySize: Int,
              indexOffset: Int,
              valueOffset: Int,
              valueLength: Int,
              sortedIndexAccessPosition: Int): T
  }

  object Remove extends Reader[Persistent.Remove] {
    def apply(key: Slice[Byte],
              deadline: Option[Deadline],
              valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
              time: Time,
              nextIndexOffset: Int,
              nextKeySize: Int,
              indexOffset: Int,
              valueOffset: Int,
              valueLength: Int,
              sortedIndexAccessPosition: Int): Persistent.Remove =
      Persistent.Remove(
        _key = key,
        indexOffset = indexOffset,
        nextIndexOffset = nextIndexOffset,
        nextKeySize = nextKeySize,
        deadline = deadline,
        sortedIndexAccessPosition = sortedIndexAccessPosition,
        _time = time
      )

  }

  case class Remove(private var _key: Slice[Byte],
                    deadline: Option[Deadline],
                    private var _time: Time,
                    indexOffset: Int,
                    nextIndexOffset: Int,
                    nextKeySize: Int,
                    sortedIndexAccessPosition: Int) extends Persistent.Fixed with KeyValue.Remove {
    override val valueLength: Int = 0
    override val isValueCached: Boolean = true
    override val valueOffset: Int = -1

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

    override def toMemory(): Memory.Remove =
      Memory.Remove(
        key = key,
        deadline = deadline,
        time = time
      )

    override def copyWithTime(time: Time): Remove =
      copy(_time = time)

    override def toFromValue(): Value.Remove =
      toRemoveValue()

    override def toRangeValue(): Value.Remove =
      toFromValue()

    override def toRemoveValue(): Value.Remove =
      Value.Remove(deadline, time)

    def toPersistent: Persistent.Remove =
      this
  }

  object Put extends Reader[Persistent.Put] {
    def apply(key: Slice[Byte],
              deadline: Option[Deadline],
              valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
              time: Time,
              nextIndexOffset: Int,
              nextKeySize: Int,
              indexOffset: Int,
              valueOffset: Int,
              valueLength: Int,
              sortedIndexAccessPosition: Int): Persistent.Put =
      new Put(
        _key = key,
        deadline = deadline,
        valueCache =
          Cache.noIO[ValuesBlock.Offset, SliceOptional[Byte]](synchronised = true, stored = true, initial = None) {
            (offset, _) =>
              if (offset.size == 0)
                Slice.Null
              else if (valuesReaderNullable == null)
                throw IO.throwable("ValuesBlock is undefined.")
              else
                UnblockedReader.moveTo(offset, valuesReaderNullable)
                  .copy()
                  .readFullBlockOrNone()
                  .unsliceOptional()
          },
        _time = time,
        nextIndexOffset = nextIndexOffset,
        nextKeySize = nextKeySize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        sortedIndexAccessPosition = sortedIndexAccessPosition
      )
  }

  case class Put(private var _key: Slice[Byte],
                 deadline: Option[Deadline],
                 private val valueCache: CacheNoIO[ValuesBlock.Offset, SliceOptional[Byte]],
                 private var _time: Time,
                 nextIndexOffset: Int,
                 nextKeySize: Int,
                 indexOffset: Int,
                 valueOffset: Int,
                 valueLength: Int,
                 sortedIndexAccessPosition: Int) extends Persistent.Fixed with KeyValue.Put {
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

    override def getOrFetchValue: SliceOptional[Byte] =
      valueCache.value(ValuesBlock.Offset(valueOffset, valueLength))

    override def isValueCached: Boolean =
      valueCache.isCached

    override def toFromValue(): Value.Put =
      Value.Put(
        value = getOrFetchValue,
        deadline = deadline,
        time = time
      )

    override def toRangeValue(): Value.RangeValue =
      throw IO.throwable("Put cannot be converted to RangeValue")

    override def toMemory(): Memory.Put =
      Memory.Put(
        key = key,
        value = getOrFetchValue,
        deadline = deadline,
        time = time
      )

    override def copyWithDeadlineAndTime(deadline: Option[Deadline],
                                         time: Time): Put =
      copy(deadline = deadline, _time = time)

    override def copyWithTime(time: Time): Put =
      copy(_time = time)

    def toPersistent: Persistent.Put =
      this
  }

  object Update extends Reader[Persistent.Update] {
    def apply(key: Slice[Byte],
              deadline: Option[Deadline],
              valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
              time: Time,
              nextIndexOffset: Int,
              nextKeySize: Int,
              indexOffset: Int,
              valueOffset: Int,
              valueLength: Int,
              sortedIndexAccessPosition: Int): Persistent.Update =
      new Update(
        _key = key,
        deadline = deadline,
        valueCache =
          Cache.noIO[ValuesBlock.Offset, SliceOptional[Byte]](synchronised = true, stored = true, initial = None) {
            (offset, _) =>
              if (offset.size == 0)
                Slice.Null
              else if (valuesReaderNullable == null)
                throw IO.throwable("ValuesBlock is undefined.")
              else
                UnblockedReader.moveTo(offset, valuesReaderNullable)
                  .copy()
                  .readFullBlockOrNone()
                  .unsliceOptional()
          },
        _time = time,
        nextIndexOffset = nextIndexOffset,
        nextKeySize = nextKeySize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        sortedIndexAccessPosition = sortedIndexAccessPosition
      )
  }

  case class Update(private var _key: Slice[Byte],
                    deadline: Option[Deadline],
                    private val valueCache: CacheNoIO[ValuesBlock.Offset, SliceOptional[Byte]],
                    private var _time: Time,
                    nextIndexOffset: Int,
                    nextKeySize: Int,
                    indexOffset: Int,
                    valueOffset: Int,
                    valueLength: Int,
                    sortedIndexAccessPosition: Int) extends Persistent.Fixed with KeyValue.Update {
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

    override def isValueCached: Boolean =
      valueCache.isCached

    def getOrFetchValue: SliceOptional[Byte] =
      valueCache.value(ValuesBlock.Offset(valueOffset, valueLength))

    override def toFromValue(): Value.Update =
      Value.Update(
        value = getOrFetchValue,
        deadline = deadline,
        time = time
      )

    override def toRangeValue(): Value.Update =
      toFromValue()

    override def toMemory(): Memory.Update =
      Memory.Update(
        key = key,
        value = getOrFetchValue,
        deadline = deadline,
        time = time
      )

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
        nextKeySize = nextKeySize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        sortedIndexAccessPosition = sortedIndexAccessPosition
      )

    override def toPut(deadline: Option[Deadline]): Persistent.Put =
      Persistent.Put(
        _key = key,
        deadline = deadline,
        valueCache = valueCache,
        _time = time,
        nextIndexOffset = nextIndexOffset,
        nextKeySize = nextKeySize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        sortedIndexAccessPosition = sortedIndexAccessPosition
      )

    def toPersistent: Persistent.Update =
      this
  }

  object Function extends Reader[Persistent.Function] {

    def apply(key: Slice[Byte],
              deadline: Option[Deadline],
              valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
              time: Time,
              nextIndexOffset: Int,
              nextKeySize: Int,
              indexOffset: Int,
              valueOffset: Int,
              valueLength: Int,
              sortedIndexAccessPosition: Int): Persistent.Function =
      apply(
        key = key,
        valuesReaderNullable = valuesReaderNullable,
        time = time,
        nextIndexOffset = nextIndexOffset,
        nextKeySize = nextKeySize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        sortedIndexAccessPosition = sortedIndexAccessPosition
      )

    def apply(key: Slice[Byte],
              valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
              time: Time,
              nextIndexOffset: Int,
              nextKeySize: Int,
              indexOffset: Int,
              valueOffset: Int,
              valueLength: Int,
              sortedIndexAccessPosition: Int): Persistent.Function =
      new Function(
        _key = key,
        valueCache =
          Cache.noIO[ValuesBlock.Offset, Slice[Byte]](synchronised = true, stored = true, initial = None) {
            (offset, _) =>
              if (valuesReaderNullable == null)
                throw IO.throwable("ValuesBlock is undefined.")
              else
                UnblockedReader.moveTo(offset, valuesReaderNullable)
                  .copy()
                  .readFullBlock()
                  .unslice()
          },
        _time = time,
        nextIndexOffset = nextIndexOffset,
        nextKeySize = nextKeySize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        sortedIndexAccessPosition = sortedIndexAccessPosition
      )
  }

  case class Function(private var _key: Slice[Byte],
                      private val valueCache: CacheNoIO[ValuesBlock.Offset, Slice[Byte]],
                      private var _time: Time,
                      nextIndexOffset: Int,
                      nextKeySize: Int,
                      indexOffset: Int,
                      valueOffset: Int,
                      valueLength: Int,
                      sortedIndexAccessPosition: Int) extends Persistent.Fixed with KeyValue.Function {
    override def unsliceKeys: Unit = {
      _key = _key.unslice()
      _time = _time.unslice()
    }

    override def key: Slice[Byte] =
      _key

    override def time: Time =
      _time

    override def indexEntryDeadline: Option[Deadline] = None

    override def isValueCached: Boolean =
      valueCache.isCached

    def getOrFetchFunction: Slice[Byte] =
      valueCache.value(ValuesBlock.Offset(valueOffset, valueLength))

    override def toFromValue(): Value.Function =
      Value.Function(
        function = getOrFetchFunction,
        time = time
      )

    override def toRangeValue(): Value.Function =
      toFromValue()

    override def toMemory(): Memory.Function =
      Memory.Function(
        key = key,
        function = getOrFetchFunction,
        time = time
      )

    override def copyWithTime(time: Time): Function =
      copy(_time = time)

    def toPersistent: Persistent.Function =
      this
  }

  object PendingApply extends Reader[Persistent.PendingApply] {

    def apply(key: Slice[Byte],
              deadline: Option[Deadline],
              valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
              time: Time,
              nextIndexOffset: Int,
              nextKeySize: Int,
              indexOffset: Int,
              valueOffset: Int,
              valueLength: Int,
              sortedIndexAccessPosition: Int): Persistent.PendingApply =
      new PendingApply(
        _key = key,
        _time = time,
        deadline = deadline,
        valueCache =
          Cache.noIO[ValuesBlock.Offset, Slice[Value.Apply]](synchronised = true, stored = true, initial = None) {
            (offset, _) =>
              if (valuesReaderNullable == null) {
                throw IO.throwable("ValuesBlock is undefined.")
              } else {
                val bytes =
                  UnblockedReader.moveTo(offset, valuesReaderNullable)
                    .copy()
                    .readFullBlock()

                ValueSerializer
                  .read[Slice[Value.Apply]](bytes)
                  .map(_.unslice)
              }
          },
        nextIndexOffset = nextIndexOffset,
        nextKeySize = nextKeySize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        sortedIndexAccessPosition = sortedIndexAccessPosition
      )
  }

  case class PendingApply(private var _key: Slice[Byte],
                          private var _time: Time,
                          deadline: Option[Deadline],
                          valueCache: CacheNoIO[ValuesBlock.Offset, Slice[Value.Apply]],
                          nextIndexOffset: Int,
                          nextKeySize: Int,
                          indexOffset: Int,
                          valueOffset: Int,
                          valueLength: Int,
                          sortedIndexAccessPosition: Int) extends Persistent.Fixed with KeyValue.PendingApply {
    override def unsliceKeys: Unit = {
      _key = _key.unslice()
      _time = _time.unslice()
    }

    override def key: Slice[Byte] =
      _key

    override def time: Time =
      _time

    override def indexEntryDeadline: Option[Deadline] = deadline

    override def isValueCached: Boolean =
      valueCache.isCached

    override def getOrFetchApplies: Slice[Value.Apply] =
      valueCache.value(ValuesBlock.Offset(valueOffset, valueLength))

    override def toFromValue(): Value.PendingApply = {
      val applies = valueCache.value(ValuesBlock.Offset(valueOffset, valueLength))
      Value.PendingApply(applies)
    }

    override def toRangeValue(): Value.PendingApply =
      toFromValue()

    override def toMemory(): Memory.PendingApply =
      Memory.PendingApply(
        key = key,
        applies = getOrFetchApplies
      )

    def toPersistent: Persistent.PendingApply =
      this
  }

  object Range extends Reader[Persistent.Range] {
    def apply(key: Slice[Byte],
              deadline: Option[Deadline],
              valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
              time: Time,
              nextIndexOffset: Int,
              nextKeySize: Int,
              indexOffset: Int,
              valueOffset: Int,
              valueLength: Int,
              sortedIndexAccessPosition: Int): Persistent.Range =
      apply(
        key = key,
        valuesReaderNullable = valuesReaderNullable,
        nextIndexOffset = nextIndexOffset,
        nextKeySize = nextKeySize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        sortedIndexAccessPosition = sortedIndexAccessPosition
      )

    def apply(key: Slice[Byte],
              valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
              nextIndexOffset: Int,
              nextKeySize: Int,
              indexOffset: Int,
              valueOffset: Int,
              valueLength: Int,
              sortedIndexAccessPosition: Int): Range = {
      val (fromKey, toKey) = Bytes.decompressJoin(key)
      Range.parsedKey(
        fromKey = fromKey,
        toKey = toKey,
        valuesReaderNullable = valuesReaderNullable,
        nextIndexOffset = nextIndexOffset,
        nextKeySize = nextKeySize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        sortedIndexAccessPosition = sortedIndexAccessPosition
      )
    }

    def parsedKey(fromKey: Slice[Byte],
                  toKey: Slice[Byte],
                  valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
                  nextIndexOffset: Int,
                  nextKeySize: Int,
                  indexOffset: Int,
                  valueOffset: Int,
                  valueLength: Int,
                  sortedIndexAccessPosition: Int): Persistent.Range =
      Range(
        _fromKey = fromKey,
        _toKey = toKey,
        valueCache =
          Cache.noIO[ValuesBlock.Offset, (Value.FromValueOption, Value.RangeValue)](synchronised = true, stored = true, initial = None) {
            (offset, _) =>
              if (valuesReaderNullable == null) {
                throw IO.throwable("ValuesBlock is undefined.")
              } else {
                val bytes =
                  UnblockedReader.moveTo(offset, valuesReaderNullable)
                    .copy()
                    .readFullBlock()

                val (from, range) =
                  RangeValueSerializer.read(bytes)

                (from.flatMapS(_.unslice), range.unslice)
              }

          },
        nextIndexOffset = nextIndexOffset,
        nextKeySize = nextKeySize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength,
        sortedIndexAccessPosition = sortedIndexAccessPosition
      )
  }

  case class Range private(private var _fromKey: Slice[Byte],
                           private var _toKey: Slice[Byte],
                           valueCache: CacheNoIO[ValuesBlock.Offset, (Value.FromValueOption, Value.RangeValue)],
                           nextIndexOffset: Int,
                           nextKeySize: Int,
                           indexOffset: Int,
                           valueOffset: Int,
                           valueLength: Int,
                           sortedIndexAccessPosition: Int) extends Persistent with KeyValue.Range with Partial.Range {

    def fromKey = _fromKey

    def toKey = _toKey

    override def indexEntryDeadline: Option[Deadline] = None

    override def unsliceKeys: Unit = {
      this._fromKey = _fromKey.unslice()
      this._toKey = _toKey.unslice()
    }

    override def key: Slice[Byte] =
      _fromKey

    def fetchRangeValueUnsafe: Value.RangeValue =
      fetchFromAndRangeValueUnsafe._2

    def fetchFromValueUnsafe: Value.FromValueOption =
      fetchFromAndRangeValueUnsafe._1

    def fetchFromAndRangeValueUnsafe: (Value.FromValueOption, Value.RangeValue) =
      valueCache.value(ValuesBlock.Offset(valueOffset, valueLength))

    override def toMemory(): Memory.Range = {
      val (fromValue, rangeValue) = fetchFromAndRangeValueUnsafe
      Memory.Range(
        fromKey = fromKey,
        toKey = toKey,
        fromValue = fromValue,
        rangeValue = rangeValue
      )
    }

    override def isValueCached: Boolean =
      valueCache.isCached

    def toPersistent: Persistent.Range =
      this
  }
}
