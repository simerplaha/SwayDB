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

package swaydb.core.data

import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.KeyValue.ReadOnly.Fixed
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.RangeValueSerializer
import swaydb.core.util.{ByteUtilCore, TryUtil}
import swaydb.data.slice.{Reader, Slice}

import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.{Failure, Success, Try}

private[core] sealed trait KeyValue {
  def key: Slice[Byte]

  def isRemove: Boolean

  def notRemove: Boolean = !isRemove

  def keyLength =
    key.size

  def getOrFetchValue: Try[Option[Slice[Byte]]]

}

private[core] object KeyValue {

  /**
    * Read-only instances are only created for Key-values read from disk for Persistent Segments
    * and are stored in-memory after merge for Memory Segments.
    */
  sealed trait ReadOnly extends KeyValue

  object ReadOnly {

    sealed trait Fixed extends ReadOnly {
      def deadline: Option[Deadline]

      def hasTimeLeft(): Boolean

      def isOverdue(): Boolean =
        !hasTimeLeft()

      def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean

      def updateDeadline(deadline: Deadline): Fixed
    }

    sealed trait Overwrite extends Fixed

    implicit class OverwriteReadOnlyImplicit(overwrite: KeyValue.ReadOnly.Fixed) {
      def toFromValue: Try[Value.FromValue] =
        overwrite match {
          case _: Memory.Remove | _: Persistent.Remove =>
            Success(Value.Remove(overwrite.deadline))

          case put: Memory.Put =>
            Success(Value.Put(put.value, put.deadline))

          case put: Persistent.Put =>
            put.getOrFetchValue.map(Value.Put(_, put.deadline))

          case put: Memory.Update =>
            Success(Value.Update(put.value, put.deadline))

          case update: Persistent.Update =>
            update.getOrFetchValue.map(Value.Update(_, update.deadline))

        }

      def toRangeValue: Try[Value.RangeValue] =
        overwrite match {
          case _: Memory.Remove | _: Persistent.Remove =>
            Success(Value.Remove(overwrite.deadline))

          case put: Memory.Put =>
            Success(Value.Update(put.value, put.deadline))

          case put: Persistent.Put =>
            put.getOrFetchValue.map(Value.Update(_, put.deadline))

          case put: Memory.Update =>
            Success(Value.Update(put.value, put.deadline))

          case update: Persistent.Update =>
            update.getOrFetchValue.map(Value.Update(_, update.deadline))

        }
    }

    //    sealed trait Response extends KeyValue.ReadOnly.Fixed {
    //      def valueLength: Int
    //
    //      def updateDeadline(deadline: Deadline): Response
    //    }

    sealed trait Put extends KeyValue.ReadOnly.Overwrite {
      def valueLength: Int

      def updateDeadline(deadline: Deadline): Put
    }

    sealed trait Remove extends KeyValue.ReadOnly.Overwrite

    sealed trait Update extends KeyValue.ReadOnly.Fixed {
      def toPut(): KeyValue.ReadOnly.Put

      def toPut(deadline: Deadline): KeyValue.ReadOnly.Put
    }

    sealed trait Range extends KeyValue.ReadOnly {
      def fromKey: Slice[Byte]

      def toKey: Slice[Byte]

      def fetchFromValue: Try[Option[Value.FromValue]]

      def fetchRangeValue: Try[Value.RangeValue]

      def fetchFromAndRangeValue: Try[(Option[Value.FromValue], Value.RangeValue)]

      def fetchFromOrElseRangeValue: Try[Value] =
        fetchFromAndRangeValue map {
          case (fromValue, rangeValue) =>
            fromValue getOrElse rangeValue
        }
    }
  }

  /**
    * Write-only instances are only created after a successful merge of key-values and are used to write to Persistent
    * and Memory Segments.
    */
  sealed trait WriteOnly extends KeyValue {

    val id: Int
    val stats: Stats
    val isRemoveRange: Boolean
    val isRange: Boolean

    def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly

  }

  object WriteOnly {

    sealed trait Fixed extends KeyValue.WriteOnly {
      def deadline: Option[Deadline]

      def hasTimeLeft(): Boolean

      def isOverdue(): Boolean =
        !hasTimeLeft()
    }

    sealed trait Overwrite extends KeyValue.WriteOnly.Fixed

    implicit class OverwriteWriteOnlyImplicit(overwrite: Overwrite) {
      def toValue: Try[Value] =
        overwrite match {
          case remove: Transient.Remove =>
            Success(Value.Remove(remove.deadline))
          case put: Transient.Put =>
            Success(Value.Put(put.value, put.deadline))
        }
    }

    sealed trait Update extends KeyValue.WriteOnly.Fixed

    sealed trait Range extends KeyValue.WriteOnly {
      val isRange: Boolean = true

      def fromKey: Slice[Byte]

      def toKey: Slice[Byte]

      def fullKey: Slice[Byte]

      def fetchFromValue: Try[Option[Value.FromValue]]

      def fetchRangeValue: Try[Value.RangeValue]

      def fetchFromAndRangeValue: Try[(Option[Value.FromValue], Value.RangeValue)]
    }
  }

  type KeyValueTuple = (Slice[Byte], Option[Slice[Byte]])
}

private[swaydb] sealed trait Memory extends KeyValue.ReadOnly {

  def key: Slice[Byte]

}

private[swaydb] object Memory {

  implicit class MemoryImplicits(memory: Memory.Fixed) {
    def toFromValue: Value.FromValue =
      memory match {
        case put: Put =>
          Value.Put(put.value, put.deadline)
        case update: Update =>
          Value.Update(update.value, update.deadline)
        case remove: Remove =>
          Value.Remove(remove.deadline)
      }

    def toRangeValue: Value.RangeValue =
      memory match {
        case put: Put =>
          Value.Update(put.value, put.deadline)
        case update: Update =>
          Value.Update(update.value, update.deadline)
        case remove: Remove =>
          Value.Remove(remove.deadline)
      }
  }

  sealed trait Fixed extends Memory with KeyValue.ReadOnly.Fixed
  sealed trait Overwrite extends Fixed

  object Put {
    def apply(key: Slice[Byte],
              value: Slice[Byte]): Put =
      new Put(key, Some(value), None)

    def apply(key: Slice[Byte],
              value: Slice[Byte],
              removeAt: Deadline): Put =
      new Put(key, Some(value), Some(removeAt))

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]],
              removeAt: Deadline): Put =
      new Put(key, value, Some(removeAt))

    def apply(key: Slice[Byte],
              value: Slice[Byte],
              removeAt: Option[Deadline]): Put =
      new Put(key, Some(value), removeAt)

    def apply(key: Slice[Byte],
              value: Slice[Byte],
              removeAfter: FiniteDuration): Put =
      new Put(key, Some(value), Some(removeAfter.fromNow))

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]]): Put =
      new Put(key, value, None)

    def apply(key: Slice[Byte]): Put =
      new Put(key, None, None)
  }

  case class Put(key: Slice[Byte],
                 value: Option[Slice[Byte]],
                 deadline: Option[Deadline]) extends Memory.Overwrite with KeyValue.ReadOnly.Put {

    override val isRemove: Boolean = false

    override def getOrFetchValue: Try[Option[Slice[Byte]]] =
      Success(value)

    override def valueLength: Int =
      value.map(_.size).getOrElse(0)

    override def updateDeadline(deadline: Deadline): Put =
      copy(deadline = Some(deadline))

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    override def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean =
      deadline.forall(deadline => (deadline - minus).hasTimeLeft())

  }

  object Update {
    def apply(key: Slice[Byte],
              value: Slice[Byte]): Update =
      new Update(key, Some(value), None)

    def apply(key: Slice[Byte],
              value: Slice[Byte],
              removeAt: Deadline): Update =
      new Update(key, Some(value), Some(removeAt))

    def apply(key: Slice[Byte],
              value: Slice[Byte],
              removeAt: Option[Deadline]): Update =
      new Update(key, Some(value), removeAt)

    def apply(key: Slice[Byte],
              value: Slice[Byte],
              removeAfter: FiniteDuration): Update =
      new Update(key, Some(value), Some(removeAfter.fromNow))

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]]): Update =
      new Update(key, value, None)

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]],
              deadline: Deadline): Update =
      new Update(key, value, Some(deadline))

    def apply(key: Slice[Byte]): Update =
      new Update(key, None, None)
  }

  case class Update(key: Slice[Byte],
                    value: Option[Slice[Byte]],
                    deadline: Option[Deadline]) extends KeyValue.ReadOnly.Update with Memory.Fixed {

    override val isRemove: Boolean = false

    override def getOrFetchValue: Try[Option[Slice[Byte]]] =
      Success(value)

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    override def updateDeadline(deadline: Deadline): Update =
      copy(deadline = Some(deadline))

    override def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean =
      deadline.forall(deadline => (deadline - minus).hasTimeLeft())

    override def toPut(): Memory.Put =
      Memory.Put(key, value, deadline)

    override def toPut(deadline: Deadline): Memory.Put =
      Memory.Put(key, value, deadline)
  }

  object Remove {
    def apply(key: Slice[Byte]): Remove =
      new Remove(key, None)

    def apply(key: Slice[Byte], deadline: Deadline): Remove =
      new Remove(key, Some(deadline))

    def apply(key: Slice[Byte], deadline: FiniteDuration): Remove =
      new Remove(key, Some(deadline.fromNow))
  }

  case class Remove(key: Slice[Byte],
                    deadline: Option[Deadline]) extends Memory.Overwrite with KeyValue.ReadOnly.Remove {

    override val isRemove: Boolean = true

    override def updateDeadline(deadline: Deadline): Remove =
      copy(deadline = Some(deadline))

    override def getOrFetchValue: Try[Option[Slice[Byte]]] =
      TryUtil.successNone

    override def hasTimeLeft(): Boolean =
      deadline.exists(_.hasTimeLeft())

    override def hasTimeLeftAtLeast(atLeast: FiniteDuration): Boolean =
      deadline.exists(deadline => (deadline - atLeast).hasTimeLeft())
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
                   rangeValue: Value.RangeValue) extends Memory with KeyValue.ReadOnly.Range {

    override val isRemove: Boolean = false

    override def key: Slice[Byte] = fromKey

    override def fetchFromValue: Try[Option[Value.FromValue]] =
      Success(fromValue)

    override def fetchRangeValue: Try[Value.RangeValue] =
      Success(rangeValue)

    override def fetchFromAndRangeValue: Try[(Option[Value.FromValue], Value.RangeValue)] =
      Success(fromValue, rangeValue)

    override def getOrFetchValue: Try[Option[Slice[Byte]]] =
      Failure(new IllegalAccessError(s"${classOf[Range].getSimpleName} do not store value."))
  }
}

private[core] sealed trait Transient extends KeyValue.WriteOnly {
  val value: Option[Slice[Byte]]
}

private[core] object Transient {

  implicit class TransientImplicits(transient: Transient) {
    def toMemory: Try[Memory] =
      transient match {
        case put: Transient.Put =>
          put.getOrFetchValue map {
            value =>
              Memory.Put(put.key, value, put.deadline)
          }

        case remove: Transient.Remove =>
          Success(Memory.Remove(remove.key, remove.deadline))

        case update: Transient.Update =>
          update.getOrFetchValue map {
            value =>
              Memory.Update(update.key, value, update.deadline)
          }

        case range: Transient.Range =>
          range.fetchFromAndRangeValue map {
            case (fromValue, rangeValue) =>
              Memory.Range(range.fromKey, range.toKey, fromValue, rangeValue)
          }
      }
  }

  object Remove {
    val id: Int = 0

    def apply(key: Slice[Byte]): Remove =
      new Remove(key, None, Stats(key, None, 0.1, isRemoveRange = false, isRange = false, None, deadlines = Seq.empty))

    def apply(key: Slice[Byte], removeAfter: FiniteDuration): Remove = {
      val deadline = removeAfter.fromNow
      new Remove(key, Some(deadline), Stats(key, None, 0.1, isRemoveRange = false, isRange = false, None, Seq(deadline)))
    }

    def apply(key: Slice[Byte], falsePositiveRate: Double): Remove =
      new Remove(key, None, Stats(key, None, falsePositiveRate, isRemoveRange = false, isRange = false, None, deadlines = Seq.empty))

    def apply(key: Slice[Byte],
              falsePositiveRate: Double,
              previous: Option[KeyValue.WriteOnly]): Remove =
      new Remove(key, None, Stats(key, None, falsePositiveRate, isRemoveRange = false, isRange = false, previous, deadlines = Seq.empty))

    def apply(key: Slice[Byte],
              falsePositiveRate: Double,
              previous: Option[KeyValue.WriteOnly],
              deadline: Option[Deadline]): Remove =
      new Remove(key, deadline, Stats(key, None, falsePositiveRate, isRemoveRange = false, isRange = false, previous, deadline))

    def apply(key: Slice[Byte],
              previous: Option[KeyValue.WriteOnly],
              deadline: Option[Deadline]): Remove =
      new Remove(key, deadline, Stats(key, None, 0.1, isRemoveRange = false, isRange = false, previous, deadline))
  }

  case class Remove(key: Slice[Byte],
                    deadline: Option[Deadline],
                    stats: Stats) extends Transient with KeyValue.WriteOnly.Overwrite {
    override val id: Int = Remove.id

    override val isRange: Boolean = false

    override val isRemoveRange =
      false

    override val value: Option[Slice[Byte]] = None

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = TryUtil.successNone

    override def updateStats(falsePositiveRate: Double,
                             keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(
        stats =
          Stats(
            key = key,
            value = None,
            falsePositiveRate = falsePositiveRate,
            isRemoveRange = isRemoveRange,
            isRange = isRange,
            previous = keyValue,
            deadlines = deadline
          )
      )

    override def isRemove: Boolean = true

    override def hasTimeLeft(): Boolean =
      deadline.exists(_.hasTimeLeft())
  }

  object Put {
    val id = 1

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]],
              falsePositiveRate: Double,
              previousMayBe: Option[KeyValue.WriteOnly]): Put =
      new Put(key, value, None, Stats(key, value, falsePositiveRate, false, false, previousMayBe, deadlines = Seq.empty))

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]],
              falsePositiveRate: Double,
              previousMayBe: Option[KeyValue.WriteOnly],
              deadline: Option[Deadline]): Put =
      new Put(key, value, deadline, Stats(key, value, falsePositiveRate, false, false, previousMayBe, deadline))

    def apply(key: Slice[Byte]): Put =
      Put(key, None, None, Stats(key, falsePositiveRate = 0.1))

    def apply(key: Slice[Byte], value: Slice[Byte]): Put =
      Put(key, Some(value), None, Stats(key, value, falsePositiveRate = 0.1))

    def apply(key: Slice[Byte],
              value: Slice[Byte],
              removeAfter: FiniteDuration): Put = {
      val deadline = removeAfter.fromNow
      Put(key, Some(value), Some(deadline), Stats(key, value, falsePositiveRate = 0.1, None, Seq(deadline)))
    }

    def apply(key: Slice[Byte],
              value: Slice[Byte],
              deadline: Deadline): Put =
      Put(key, Some(value), Some(deadline), Stats(key, value, falsePositiveRate = 0.1, None, deadlines = Seq(deadline)))

    def apply(key: Slice[Byte],
              removeAfter: FiniteDuration): Put = {
      val deadline = removeAfter.fromNow
      Put(key, None, Some(deadline), Stats(key, falsePositiveRate = 0.1, Seq(deadline)))
    }

    def apply(key: Slice[Byte],
              value: Slice[Byte],
              removeAfter: Option[FiniteDuration]): Put = {
      val deadline = removeAfter.map(_.fromNow)
      Put(key, Some(value), deadline, Stats(key, value, falsePositiveRate = 0.1, None, deadline))
    }

    def apply(key: Slice[Byte], value: Slice[Byte], falsePositiveRate: Double): Put =
      Put(key, Some(value), None, Stats(key, value, falsePositiveRate = falsePositiveRate))

    def apply(key: Slice[Byte], falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Put =
      Put(key, None, falsePositiveRate, previous)

    def apply(key: Slice[Byte],
              previous: Option[KeyValue.WriteOnly],
              deadline: Option[Deadline]): Put =
      Put(key, None, deadline, Stats(key, falsePositiveRate = 0.1, previous, deadline))

    def apply(key: Slice[Byte], value: Slice[Byte], falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Put =
      Put(key, Some(value), None, Stats(key, value, falsePositiveRate = falsePositiveRate, previous, deadlines = Seq.empty))
  }

  case class Put(key: Slice[Byte],
                 value: Option[Slice[Byte]],
                 deadline: Option[Deadline],
                 stats: Stats) extends Transient with KeyValue.WriteOnly.Overwrite {

    override val id: Int = Put.id

    override val isRemoveRange =
      false

    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(
        stats =
          Stats(
            key = key,
            value = value,
            falsePositiveRate = falsePositiveRate,
            isRemoveRange = isRemoveRange,
            isRange = isRange,
            previous = keyValue,
            deadlines = deadline
          )
      )

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = Success(value)

    override def isRemove: Boolean = false

    override val isRange: Boolean = false

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())
  }

  object Update {
    val id = 2

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]],
              falsePositiveRate: Double,
              previousMayBe: Option[KeyValue.WriteOnly]): Update =
      new Update(key, value, None, Stats(key, value, falsePositiveRate, false, false, previousMayBe, deadlines = Seq.empty))

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]],
              falsePositiveRate: Double,
              previousMayBe: Option[KeyValue.WriteOnly],
              deadline: Option[Deadline]): Update =
      new Update(key, value, deadline, Stats(key, value, falsePositiveRate, false, false, previousMayBe, deadline))

    def apply(key: Slice[Byte]): Update =
      Update(key, None, None, Stats(key, falsePositiveRate = 0.1))

    def apply(key: Slice[Byte], value: Slice[Byte]): Update =
      Update(key, Some(value), None, Stats(key, value, falsePositiveRate = 0.1))

    def apply(key: Slice[Byte],
              value: Slice[Byte],
              removeAfter: FiniteDuration): Update = {
      val deadline = removeAfter.fromNow
      Update(key, Some(value), Some(deadline), Stats(key, value, falsePositiveRate = 0.1, None, Seq(deadline)))
    }

    def apply(key: Slice[Byte],
              value: Slice[Byte],
              deadline: Deadline): Update =
      Update(key, Some(value), Some(deadline), Stats(key, value, falsePositiveRate = 0.1, None, deadlines = Seq(deadline)))

    def apply(key: Slice[Byte],
              removeAfter: FiniteDuration): Update = {
      val deadline = removeAfter.fromNow
      Update(key, None, Some(deadline), Stats(key, falsePositiveRate = 0.1, Seq(deadline)))
    }

    def apply(key: Slice[Byte],
              value: Slice[Byte],
              removeAfter: Option[FiniteDuration]): Update = {
      val deadline = removeAfter.map(_.fromNow)
      Update(key, Some(value), deadline, Stats(key, value, falsePositiveRate = 0.1, None, deadline))
    }

    def apply(key: Slice[Byte], value: Slice[Byte], falsePositiveRate: Double): Update =
      Update(key, Some(value), None, Stats(key, value, falsePositiveRate = falsePositiveRate))

    def apply(key: Slice[Byte], falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Update =
      Update(key, None, falsePositiveRate, previous)

    def apply(key: Slice[Byte],
              previous: Option[KeyValue.WriteOnly],
              deadline: Option[Deadline]): Update =
      Update(key, None, deadline, Stats(key, falsePositiveRate = 0.1, previous, deadline))

    def apply(key: Slice[Byte], value: Slice[Byte], falsePositiveRate: Double, previous: Option[KeyValue.WriteOnly]): Update =
      Update(key, Some(value), None, Stats(key, value, falsePositiveRate = falsePositiveRate, previous, deadlines = Seq.empty))
  }

  case class Update(key: Slice[Byte],
                    value: Option[Slice[Byte]],
                    deadline: Option[Deadline],
                    stats: Stats) extends Transient with KeyValue.WriteOnly.Update {

    override val id: Int = Update.id

    override val isRemoveRange =
      false

    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(
        stats =
          Stats(
            key = key,
            value = value,
            falsePositiveRate = falsePositiveRate,
            isRemoveRange = isRemoveRange,
            isRange = isRange,
            previous = keyValue,
            deadlines = deadline
          )
      )

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = Success(value)

    override def isRemove: Boolean = false

    override val isRange: Boolean = false

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())
  }

  object Range {

    def apply[F <: Value.FromValue, R <: Value.RangeValue](fromKey: Slice[Byte],
                                                           toKey: Slice[Byte],
                                                           fromValue: Option[F],
                                                           rangeValue: R)(implicit rangeValueSerializer: RangeValueSerializer[Option[F], R]): Range =
      Range(fromKey = fromKey, toKey = toKey, fromValue = fromValue, rangeValue = rangeValue, falsePositiveRate = 0.1, previous = None)

    def apply[R <: Value.RangeValue](fromKey: Slice[Byte],
                                     toKey: Slice[Byte],
                                     rangeValue: R,
                                     falsePositiveRate: Double,
                                     previous: Option[KeyValue.WriteOnly])(implicit rangeValueSerializer: RangeValueSerializer[Unit, R]): Range = {
      val (bytesRequired, rangeId) = rangeValueSerializer.bytesRequiredAndRangeId((), rangeValue)
      val value = if (bytesRequired == 0) None else Some(Slice.create[Byte](bytesRequired))
      value.foreach(rangeValueSerializer.write((), rangeValue, _))
      val fullKey = ByteUtilCore.compress(fromKey, toKey)
      new Range(
        id = rangeId,
        fromKey = fromKey,
        toKey = toKey,
        fullKey = fullKey,
        fromValue = None,
        rangeValue = rangeValue,
        value = value,
        stats =
          Stats(
            key = fullKey,
            value = value,
            falsePositiveRate = falsePositiveRate,
            isRemoveRange = rangeValue.isRemove,
            isRange = true,
            previous = previous,
            deadlines = Seq.empty
          )
      )
    }

    def apply[F <: Value.FromValue, R <: Value.RangeValue](fromKey: Slice[Byte],
                                                           toKey: Slice[Byte],
                                                           fromValue: Option[F],
                                                           rangeValue: R,
                                                           falsePositiveRate: Double,
                                                           previous: Option[KeyValue.WriteOnly])(implicit rangeValueSerializer: RangeValueSerializer[Option[F], R]): Range = {
      val (bytesRequired, rangeId) = rangeValueSerializer.bytesRequiredAndRangeId(fromValue, rangeValue)
      val value = if (bytesRequired == 0) None else Some(Slice.create[Byte](bytesRequired))
      value.foreach(rangeValueSerializer.write(fromValue, rangeValue, _))
      val fullKey: Slice[Byte] = ByteUtilCore.compress(fromKey, toKey)

      new Range(
        id = rangeId,
        fromKey = fromKey,
        fullKey = fullKey,
        toKey = toKey,
        fromValue = fromValue,
        rangeValue = rangeValue,
        value = value,
        stats =
          Stats(
            key = fullKey,
            value = value,
            falsePositiveRate = falsePositiveRate,
            isRemoveRange = rangeValue.isRemove,
            isRange = true,
            previous = previous,
            deadlines = Seq.empty
          )
      )
    }
  }

  case class Range(id: Int,
                   fromKey: Slice[Byte],
                   toKey: Slice[Byte],
                   fullKey: Slice[Byte],
                   fromValue: Option[Value.FromValue],
                   rangeValue: Value.RangeValue,
                   value: Option[Slice[Byte]],
                   stats: Stats) extends Transient with KeyValue.WriteOnly.Range {

    override def key = fromKey

    override val isRemoveRange =
      rangeValue.isRemove

    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly =
      this.copy(
        stats =
          Stats(
            key = fullKey,
            value = value,
            falsePositiveRate = falsePositiveRate,
            isRemoveRange = isRemoveRange,
            isRange = true,
            previous = keyValue,
            deadlines = Seq.empty
          )
      )

    override def getOrFetchValue: Try[Option[Slice[Byte]]] =
      Success(value)

    override def fetchFromValue: Try[Option[Value.FromValue]] =
      Success(fromValue)

    override def fetchRangeValue: Try[Value.RangeValue] =
      Success(rangeValue)

    override def fetchFromAndRangeValue: Try[(Option[Value.FromValue], Value.RangeValue)] =
      Success(fromValue, rangeValue)

    override def isRemove: Boolean = false
  }
}

private[core] sealed trait Persistent extends KeyValue.ReadOnly {

  val indexOffset: Int
  val nextIndexOffset: Int
  val nextIndexSize: Int

  def key: Slice[Byte]

  def isValueDefined: Boolean

  def getOrFetchValue: Try[Option[Slice[Byte]]]

  def valueLength: Int

  def unsliceKey: Unit
}

private[core] object Persistent {

  sealed trait Fixed extends Persistent with KeyValue.ReadOnly.Fixed
  sealed trait Overwrite extends Fixed with KeyValue.ReadOnly.Overwrite

  object Remove {
    def apply(indexOffset: Int)(key: Slice[Byte],
                                nextIndexOffset: Int,
                                nextIndexSize: Int,
                                deadline: Option[Deadline]): Remove =
      Remove(key, deadline, indexOffset, nextIndexOffset, nextIndexSize)

    def apply(key: Slice[Byte]): Remove =
      Remove(key, None, 0, 0, 0)

  }

  case class Remove(private var _key: Slice[Byte],
                    deadline: Option[Deadline],
                    indexOffset: Int,
                    nextIndexOffset: Int,
                    nextIndexSize: Int) extends Persistent.Overwrite with KeyValue.ReadOnly.Remove {
    def key = _key

    override def unsliceKey(): Unit =
      _key = _key.unslice()

    override def updateDeadline(deadline: Deadline): Remove =
      copy(deadline = Some(deadline))

    override val valueLength: Int = 0

    override def isValueDefined: Boolean = true

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = TryUtil.successNone

    override def isRemove: Boolean = true

    override def hasTimeLeft(): Boolean =
      deadline.exists(_.hasTimeLeft())

    override def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean =
      deadline.exists(deadline => (deadline - minus).hasTimeLeft())
  }

  object Put {
    def apply(valueReader: Reader,
              indexOffset: Int)(key: Slice[Byte],
                                valueLength: Int,
                                valueOffset: Int,
                                nextIndexOffset: Int,
                                nextIndexSize: Int,
                                deadline: Option[Deadline]): Put =
      Put(key, deadline, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength)

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]]): Put =
      value match {
        case Some(value) =>
          Put(key, None, Reader(value), 0, 0, 0, 0, value.size)
        case None =>
          Put(key, None, Reader.emptyReader, 0, 0, 0, 0, 0)
      }
  }

  case class Put(private var _key: Slice[Byte],
                 deadline: Option[Deadline],
                 valueReader: Reader,
                 nextIndexOffset: Int,
                 nextIndexSize: Int,
                 indexOffset: Int,
                 valueOffset: Int,
                 valueLength: Int) extends Persistent.Overwrite with LazyValue with KeyValue.ReadOnly.Put {
    override def unsliceKey: Unit =
      _key = _key.unslice()

    override def key: Slice[Byte] =
      _key

    override def updateDeadline(deadline: Deadline): Put =
      copy(deadline = Some(deadline))

    override def isRemove: Boolean =
      false

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    override def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean =
      deadline.forall(deadline => (deadline - minus).hasTimeLeft())
  }

  object Update {
    def apply(valueReader: Reader,
              indexOffset: Int)(key: Slice[Byte],
                                valueLength: Int,
                                valueOffset: Int,
                                nextIndexOffset: Int,
                                nextIndexSize: Int,
                                deadline: Option[Deadline]): Update =
      Update(key, deadline, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength)

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]]): Update =
      value match {
        case Some(value) =>
          Update(key, None, Reader(value), 0, 0, 0, 0, value.size)
        case None =>
          Update(key, None, Reader.emptyReader, 0, 0, 0, 0, 0)
      }
  }

  case class Update(private var _key: Slice[Byte],
                    deadline: Option[Deadline],
                    valueReader: Reader,
                    nextIndexOffset: Int,
                    nextIndexSize: Int,
                    indexOffset: Int,
                    valueOffset: Int,
                    valueLength: Int) extends Persistent with Persistent.Fixed with LazyValue with KeyValue.ReadOnly.Update {
    override def unsliceKey: Unit =
      _key = _key.unslice()

    override def key: Slice[Byte] =
      _key

    override def isRemove: Boolean =
      false

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    override def updateDeadline(deadline: Deadline): Update =
      copy(deadline = Some(deadline))

    override def hasTimeLeftAtLeast(minus: FiniteDuration): Boolean =
      deadline.forall(deadline => (deadline - minus).hasTimeLeft())

    override def toPut(): Persistent.Put =
      Persistent.Put(
        _key = key,
        deadline = deadline,
        valueReader = valueReader,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength
      )

    override def toPut(deadline: Deadline): Persistent.Put =
      Persistent.Put(
        _key = key,
        deadline = Some(deadline),
        valueReader = valueReader,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        indexOffset = indexOffset,
        valueOffset = valueOffset,
        valueLength = valueLength
      )
  }

  object Range {
    def apply(valueReader: Reader,
              indexOffset: Int)(id: Int,
                                key: Slice[Byte],
                                valueLength: Int,
                                valueOffset: Int,
                                nextIndexOffset: Int,
                                nextIndexSize: Int): Try[Range] =
      ByteUtilCore.uncompress(key) map {
        case (fromKey, toKey) =>
          Range(id, fromKey, toKey, valueReader, nextIndexOffset, nextIndexSize, indexOffset, valueOffset, valueLength)
      }
  }

  case class Range(id: Int,
                   private var _fromKey: Slice[Byte],
                   private var _toKey: Slice[Byte],
                   valueReader: Reader,
                   nextIndexOffset: Int,
                   nextIndexSize: Int,
                   indexOffset: Int,
                   valueOffset: Int,
                   valueLength: Int) extends Persistent with LazyRangeValue with KeyValue.ReadOnly.Range {

    def fromKey = _fromKey

    def toKey = _toKey

    override def unsliceKey: Unit = {
      this._fromKey = _fromKey.unslice()
      this._toKey = _toKey.unslice()
    }

    override def key: Slice[Byte] =
      _fromKey

    override def isRemove: Boolean = false
  }
}